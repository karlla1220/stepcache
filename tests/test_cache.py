import hashlib
import json
import socket
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from importlib.metadata import version

import pytest

from stepcache import Blob, Refused, Step, StepCache, Version


@pytest.fixture
def cache(store):
    return StepCache(store)


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def spec_digest(value) -> str:
    text = json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False)
    return hashlib.sha256(text.encode()).hexdigest()


def at(day: int, hour: int) -> datetime:
    return datetime(2026, 1, day, hour, tzinfo=timezone.utc)


def counted(fn):
    calls = []

    def run(inputs):
        calls.append(inputs)
        return fn(inputs)

    return run, calls


def double(inputs):
    return {'y': inputs['x'] * 2}


def test_a_second_run_with_the_same_inputs_is_a_hit(cache):
    run, calls = counted(lambda inputs: {'png': Blob(b'png of ' + inputs['name'].encode()), 'kind': 'figure'})
    step = Step('render', Version.of(), run)
    first = cache.run(step, {'name': 'a'})
    second = cache.run(step, {'name': 'a'})
    assert (first.hit, second.hit) == (False, True)
    assert len(calls) == 1
    assert second.status == 'ok' and second.refusal is None
    assert second.outputs == {'png': Blob(b'png of a'), 'kind': 'figure'}
    assert second.outputs['png'].read() == b'png of a'
    assert (second.key, second.result_sha) == (first.key, first.result_sha)


def test_changing_any_part_of_the_step_changes_the_key(cache, tmp_path, load_module):
    (tmp_path / 'prompt.txt').write_text('describe the figure')
    files = Version.files(tmp_path, ['prompt.txt'])
    a = load_module('steps_a', 'def run(inputs):\n    return {}\n')
    b = load_module('steps_b', 'def run(inputs):\n    return {}  # edited\n')

    def key(name='s', run=a.run, **version):
        spec = {'files': files, 'tools': {'model': 'm1'}, 'rev': '1'} | version
        return cache.run(Step(name, Version.of(**spec), run), {'x': 1}).key

    base = key()
    (tmp_path / 'prompt.txt').write_text('describe the figure briefly')
    changed = {
        'name': key(name='t'),
        'code': key(run=b.run),
        'files': key(files=Version.files(tmp_path, ['prompt.txt'])),
        'tools': key(tools={'model': 'm2'}),
        'rev': key(rev='2'),
    }
    assert key() == base
    assert [part for part, k in changed.items() if k == base] == []


def test_input_order_and_file_names_do_not_change_the_key(cache, tmp_path):
    (tmp_path / 'figure1.png').write_bytes(b'same image')
    (tmp_path / 'copy of figure1.png').write_bytes(b'same image')
    run, calls = counted(lambda inputs: {'size': inputs['size']})
    step = Step('s', Version.of(), run)
    first = cache.run(step, {
        'png': Blob((tmp_path / 'figure1.png').read_bytes()), 'size': 2, 'crop': {'x': 0, 'y': 1},
    })
    second = cache.run(step, {
        'crop': {'y': 1, 'x': 0}, 'size': 2, 'png': Blob((tmp_path / 'copy of figure1.png').read_bytes()),
    })
    assert second.hit and second.key == first.key
    assert len(calls) == 1


def test_a_json_input_shaped_like_a_blob_reference_is_not_the_blob(cache):
    step = Step('s', Version.of(), lambda inputs: {})
    blob = Blob(b'image')
    assert cache.run(step, {'x': blob}).key != cache.run(step, {'x': ['blob', blob.sha256]}).key


def test_keys_and_hashes_follow_the_stepcache_1_format(cache, store):
    step = Step('s', Version.of(tools={'model': 'm1'}, rev='1'), lambda inputs: {'png': Blob(b'png'), 'kind': 'figure'})
    res = cache.run(step, {'wmf': Blob(b'wmf'), 'note': 'café'})
    inputs = {'wmf': ['blob', sha(b'wmf')], 'note': ['json', 'café']}
    outputs = {'png': ['blob', sha(b'png')], 'kind': ['json', 'figure']}
    assert step.version.sha == spec_digest(step.version.components)
    assert res.key == spec_digest(['stepcache/1', 's', step.version.sha, inputs])
    assert res.result_sha == spec_digest(['ok', outputs, None])
    record = store.get_record(res.key)
    assert record.input_key == spec_digest(['stepcache/1', 's', inputs])
    assert (record.step, record.version_sha, record.status, record.refusal) == ('s', step.version.sha, 'ok', None)
    assert (record.inputs, record.outputs, record.result_sha) == (inputs, outputs, res.result_sha)
    assert record.producer == {'host': socket.gethostname(), 'stepcache': version('stepcache'), 'imported': False}
    assert store.get_version(step.version.sha) == step.version.components


def test_a_later_step_hits_when_an_earlier_step_changes_but_its_output_bytes_do_not(cache):
    render, renders = counted(lambda inputs: {'png': Blob(b'png of ' + inputs['wmf'].read())})
    ocr, ocrs = counted(lambda inputs: {'text': Blob(inputs['png'].read().upper())})
    for rev in ('1', '2'):
        png = cache.run(Step('render', Version.of(rev=rev), render), {'wmf': Blob(b'figure')}).outputs['png']
        text = cache.run(Step('ocr', Version.of(), ocr), {'png': png})
    assert (len(renders), len(ocrs)) == (2, 1)
    assert text.hit and text.outputs['text'].read() == b'PNG OF FIGURE'


def test_a_refusal_is_stored_and_not_asked_again(cache):
    run, calls = counted(lambda inputs: Refused('not a figure', {'page': 3}))
    step = Step('s', Version.of(), run)
    first = cache.run(step, {'x': 1})
    second = cache.run(step, {'x': 1})
    assert len(calls) == 1 and second.hit
    for res in (first, second):
        assert (res.status, res.outputs, res.refusal) == ('refused', {}, Refused('not a figure', {'page': 3}))


def test_an_exception_is_not_stored_and_the_next_run_tries_again(cache):
    answers = iter([RuntimeError('model timed out'), {'y': 1}])

    def answer(inputs):
        value = next(answers)
        if isinstance(value, Exception):
            raise value
        return value

    run, calls = counted(answer)
    step = Step('s', Version.of(), run)
    with pytest.raises(RuntimeError):
        cache.run(step, {'x': 1})
    res = cache.run(step, {'x': 1})
    assert not res.hit and res.outputs == {'y': 1}
    assert len(calls) == 2


def test_a_record_whose_blob_is_gone_is_replaced_by_the_new_answer(cache, store):
    calls = []

    def run(inputs):
        calls.append(inputs)
        return {'alt': Blob(f'answer {len(calls)}'.encode())}

    step = Step('vlm', Version.of(), run)
    first = cache.run(step, {'x': 1})
    store.delete(first.outputs['alt'].sha256)
    second = cache.run(step, {'x': 1})
    third = cache.run(step, {'x': 1})
    assert (second.hit, third.hit) == (False, True)
    assert len(calls) == 2
    assert third.outputs['alt'].read() == b'answer 2'


def test_a_json_output_over_4_kib_is_an_error_and_stores_nothing(cache, store):
    fits = 4096 - len('""')  # a JSON string's canonical form adds its two quotes
    png = Blob(b'png')
    run, calls = counted(lambda inputs: {'png': png, 'alt': 'x' * inputs['n']})
    step = Step('s', Version.of(), run)
    for _ in range(2):
        with pytest.raises(ValueError):
            cache.run(step, {'n': fits + 1})
    assert len(calls) == 2
    assert not store.has(png.sha256)
    assert not cache.run(step, {'n': fits}).hit
    assert cache.run(step, {'n': fits}).hit


def test_run_returns_the_result_another_writer_stored_first(cache):
    answers = iter(['mine', 'theirs'])

    def run(inputs):
        answer = next(answers)
        if answer == 'mine':
            cache.run(step, inputs)
        return {'alt': answer}

    step = Step('vlm', Version.of(), run)
    res = cache.run(step, {'x': 1})
    assert not res.hit and res.outputs == {'alt': 'theirs'}
    assert cache.run(step, {'x': 1}).outputs == {'alt': 'theirs'}


def test_invalidate_makes_the_next_run_compute_again(cache):
    run, calls = counted(double)
    step = Step('s', Version.of(), run)
    cache.invalidate(step, cache.run(step, {'x': 1}).key)
    res = cache.run(step, {'x': 1})
    assert not res.hit and len(calls) == 2
    assert cache.run(step, {'x': 1}).hit


@pytest.mark.parametrize(
    'value',
    [float('nan'), [0.5, float('nan')], float('inf'), (1, 2), {1: 'a'}, object()],
    ids=['nan', 'nested-nan', 'inf', 'tuple', 'int-key', 'object'],
)
def test_an_input_that_is_not_json_is_an_error(cache, value):
    run, calls = counted(double)
    with pytest.raises(ValueError):
        cache.run(Step('s', Version.of(), run), {'x': value})
    assert calls == []


def test_a_hit_updates_last_used_at_at_most_once_a_day(cache, store, clock):
    step = Step('s', Version.of(), lambda inputs: {'png': Blob(b'png')})
    key = cache.run(step, {}).key

    def last_used():
        with closing(sqlite3.connect(store.path)) as c:
            (blob,) = c.execute('SELECT last_used_at FROM blobs WHERE sha = ?', (sha(b'png'),)).fetchone()
        return datetime.fromisoformat(store.get_record(key).last_used_at), datetime.fromisoformat(blob)

    assert last_used() == (at(1, 9), at(1, 9))
    clock.now = at(2, 10)
    assert cache.run(step, {}).hit
    assert last_used() == (at(2, 10), at(2, 10))
    clock.now = at(2, 23)
    assert cache.run(step, {}).hit
    assert last_used() == (at(2, 10), at(2, 10))


def test_a_cache_false_step_runs_every_time_and_stores_nothing(cache, store):
    run, calls = counted(lambda inputs: {'outline': Blob(b'outline')})
    step = Step('outline', Version.of(), run, cache=False)
    results = [cache.run(step, {'x': 1}) for _ in range(2)]
    assert len(calls) == 2
    assert [(r.hit, r.key, r.result_sha) for r in results] == [(False, None, None)] * 2
    assert results[0].outputs == {'outline': Blob(b'outline')}
    assert not store.has(sha(b'outline'))
