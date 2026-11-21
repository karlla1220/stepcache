import asyncio
import functools
import gc
import hashlib
import json
import multiprocessing
import os
import secrets
import signal
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from collections import Counter
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path

import pytest

import stepcache.cache
from stepcache import Blob, Refused, SqliteStore, Step, StepCache, Version


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


@pytest.mark.parametrize(
    'call',
    [StepCache.run, lambda cache, step, inputs: asyncio.run(cache.async_run(step, inputs))],
    ids=['run', 'async_run'],
)
def test_an_exception_is_not_stored_and_the_next_run_tries_again(cache, call):
    answers = iter([RuntimeError('model timed out'), {'y': 1}])

    def answer(inputs):
        value = next(answers)
        if isinstance(value, Exception):
            raise value
        return value

    run, calls = counted(answer)
    step = Step('s', Version.of(), run)
    with pytest.raises(RuntimeError):
        call(cache, step, {'x': 1})
    res = call(cache, step, {'x': 1})
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


def test_run_returns_the_result_another_writer_stored_first(cache, store):
    answers = iter(['mine', 'theirs'])

    def run(inputs):
        answer = next(answers)
        if answer == 'mine':
            # Lose the claim, as when a waiter judged this worker dead, so the inner run computes.
            with closing(sqlite3.connect(store.path)) as c, c:
                c.execute('DELETE FROM claims')
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



def test_a_cache_false_step_that_returns_a_list_is_an_error(cache):
    with pytest.raises(TypeError):
        cache.run(Step('s', Version.of(), lambda inputs: [inputs['x']], cache=False), {'x': 1})

def test_stepcache_imports_without_installed_metadata():
    src = str(Path(__file__).parents[1] / 'src')
    # -S drops site-packages, so the installed package's metadata is not found.
    code = f'import sys; sys.path.insert(0, {src!r}); import stepcache'
    assert subprocess.run([sys.executable, '-I', '-S', '-c', code]).returncode == 0


@pytest.mark.skipif(os.name != 'posix', reason='needs os.fork')
def test_a_forked_child_keeps_its_result_after_the_parent_closes_the_store(tmp_path):
    def open_cache():
        return StepCache(SqliteStore(tmp_path / 'stepcache.db', files=tmp_path / 'blobs'))

    step = Step('s', Version.of(), lambda inputs: {'png': Blob(f'png {inputs["x"]}'.encode())})
    cache = open_cache()
    cache.run(step, {'x': 1})
    parent_closed, notify = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.read(parent_closed, 1)
        code = 1
        try:
            cache.run(step, {'x': 2})
            code = 0
        finally:
            os._exit(code)
    # The parent's last connection checkpoints and deletes the WAL as it closes, so a child
    # still writing through that inherited connection would lose its result. A connection is
    # in a reference cycle, so only the collector closes it.
    del cache
    gc.collect()
    os.write(notify, b'x')
    os.close(parent_closed)
    os.close(notify)
    _, status = os.waitpid(pid, 0)
    assert os.waitstatus_to_exitcode(status) == 0
    assert open_cache().run(step, {'x': 2}).hit


def open_cache(root) -> StepCache:
    return StepCache(SqliteStore(Path(root) / 'stepcache.db', files=Path(root) / 'blobs'))


def logged(inputs):
    with open(inputs['log'], 'a') as f:
        f.write(f'{inputs["x"]}\n')
    time.sleep(inputs['sleep'])
    return {'token': secrets.token_hex(8)}


LOGGED = Step('logged', Version.of(), logged)


def executions(log) -> Counter:
    return Counter(Path(log).read_text().split())


def run_logged(cache, log, xs, sleep) -> dict:
    return {x: cache.run(LOGGED, {'x': x, 'log': str(log), 'sleep': sleep}).result_sha for x in xs}


def rotated(xs: list, i: int) -> list:
    return xs[i % len(xs):] + xs[:i % len(xs)]


def run_logged_in_process(root, barrier, xs, out):
    stepcache.cache.WAIT_SECONDS = 0.02
    cache = open_cache(root)
    barrier.wait()
    Path(out).write_text(json.dumps(run_logged(cache, Path(root) / 'log', xs, sleep=0.1)))


def test_processes_running_the_same_keys_at_once_run_each_key_once(tmp_path):
    n, xs = 6, list(range(4))
    barrier = multiprocessing.Barrier(n)
    procs = [
        multiprocessing.Process(
            target=run_logged_in_process, args=(tmp_path, barrier, rotated(xs, i), tmp_path / f'shas-{i}.json')
        )
        for i in range(n)
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join(60)
    assert [p.exitcode for p in procs] == [0] * n
    assert executions(tmp_path / 'log') == Counter({str(x): 1 for x in xs})
    shas = [json.loads((tmp_path / f'shas-{i}.json').read_text()) for i in range(n)]
    assert all(s == shas[0] for s in shas)


def test_threads_sharing_a_cache_run_each_key_once(cache, tmp_path, monkeypatch):
    monkeypatch.setattr('stepcache.cache.WAIT_SECONDS', 0.01)
    n, xs = 8, list(range(10))
    barrier = threading.Barrier(n)

    def worker(i):
        barrier.wait()
        return run_logged(cache, tmp_path / 'log', rotated(xs, i), sleep=0.01)

    with ThreadPoolExecutor(n) as pool:
        shas = list(pool.map(worker, range(n)))
    assert executions(tmp_path / 'log') == Counter({str(x): 1 for x in xs})
    assert all(s == shas[0] for s in shas)


def wait_until(condition, timeout=30):
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline
        time.sleep(0.01)


def in_thread(fn, *args) -> Future:
    """Run `fn` in a daemon thread, so that a test failing on a hang does not hang the session."""
    future = Future()

    def target():
        try:
            future.set_result(fn(*args))
        except BaseException as e:
            future.set_exception(e)

    threading.Thread(target=target, daemon=True).start()
    return future


def blocks_first(inputs):
    with open(inputs['log'], 'a') as f:
        f.write('ran\n')
    if Path(inputs['log']).read_text().count('\n') == 1:
        time.sleep(60)
    return {'pid': os.getpid()}


BLOCKS_FIRST = Step('blocks_first', Version.of(), blocks_first)


def run_blocks_first_in_process(root, started):
    stepcache.cache.WAIT_SECONDS = 0.02
    cache = open_cache(root)
    started.set()
    cache.run(BLOCKS_FIRST, {'log': str(Path(root) / 'log')})


def test_a_waiting_process_takes_over_the_claim_of_a_killed_owner_at_once(tmp_path):
    log = tmp_path / 'log'
    owner_started, waiter_started = multiprocessing.Event(), multiprocessing.Event()
    owner = multiprocessing.Process(target=run_blocks_first_in_process, args=(tmp_path, owner_started))
    waiter = multiprocessing.Process(target=run_blocks_first_in_process, args=(tmp_path, waiter_started))
    owner.start()
    try:
        wait_until(lambda: log.exists() and log.read_text() == 'ran\n')
        waiter.start()
        waiter_started.wait(30)
        time.sleep(0.3)
        assert waiter.is_alive()
        os.kill(owner.pid, signal.SIGKILL)
        owner.join()
        waiter.join(5)
        assert waiter.exitcode == 0
        res = open_cache(tmp_path).run(BLOCKS_FIRST, {'log': str(log)})
        assert res.hit and res.outputs == {'pid': waiter.pid}
    finally:
        owner.kill()
        waiter.kill()


def test_a_waiting_worker_runs_the_step_itself_when_the_owner_raises(cache, monkeypatch):
    monkeypatch.setattr('stepcache.cache.WAIT_SECONDS', 0.01)
    owner_running, waiter_started = threading.Event(), threading.Event()
    calls = []

    def run(inputs):
        calls.append(inputs)
        if len(calls) == 1:
            owner_running.set()
            waiter_started.wait(10)
            time.sleep(0.1)
            raise RuntimeError('model timed out')
        return {'y': 1}

    step = Step('s', Version.of(), run)
    owner = in_thread(cache.run, step, {'x': 1})
    owner_running.wait(10)
    waiter_started.set()
    waiter = in_thread(cache.run, step, {'x': 1})
    with pytest.raises(RuntimeError):
        owner.result(10)
    res = waiter.result(10)
    assert not res.hit and res.outputs == {'y': 1}
    assert len(calls) == 2


def test_ten_async_runs_of_one_key_run_the_step_once_and_leave_the_loop_free(cache, monkeypatch):
    monkeypatch.setattr('stepcache.cache.WAIT_SECONDS', 0.2)
    calls = []

    async def run(inputs):
        calls.append(inputs)
        await asyncio.sleep(0.5)
        return {'alt': Blob(b'alt text')}

    step = Step('vlm', Version.of(), run)

    async def main():
        ticks = []

        async def tick():
            while True:
                ticks.append(time.monotonic())
                await asyncio.sleep(0.01)

        ticker = asyncio.create_task(tick())
        results = await asyncio.gather(*(cache.async_run(step, {'x': 1}) for _ in range(10)))
        ticker.cancel()
        return results, ticks

    results, ticks = in_thread(asyncio.run, main()).result(10)
    assert len(calls) == 1
    assert sorted(r.hit for r in results) == [False] + [True] * 9
    assert len({r.result_sha for r in results}) == 1
    assert results[0].outputs['alt'].read() == b'alt text'
    assert max(b - a for a, b in zip(ticks, ticks[1:])) < 0.1


async def answer_async(inputs, scale=1):
    return {'y': inputs['x'] * scale}


@pytest.mark.parametrize(
    'run, y', [(answer_async, 1), (functools.partial(answer_async, scale=2), 2)], ids=['async-def', 'partial']
)
def test_run_and_run_many_refuse_an_async_step_and_async_run_awaits_it(cache, run, y):
    step = Step('s', Version.of(), run)
    with pytest.raises(TypeError):
        cache.run(step, {'x': 1})
    with pytest.raises(TypeError):
        cache.run_many(step, [{'x': 1}])
    first = asyncio.run(cache.async_run(step, {'x': 1}))
    second = asyncio.run(cache.async_run(step, {'x': 1}))
    assert (first.hit, second.hit) == (False, True)
    assert first.outputs == second.outputs == {'y': y}


def test_async_run_calls_a_sync_step_and_shares_its_records_with_run(cache):
    run, calls = counted(double)
    step = Step('s', Version.of(), run)
    first = asyncio.run(cache.async_run(step, {'x': 2}))
    second = cache.run(step, {'x': 2})
    assert (first.hit, second.hit) == (False, True)
    assert first.outputs == {'y': 4} and first.result_sha == second.result_sha
    assert len(calls) == 1


def test_async_run_awaits_a_cache_false_step_every_time_and_stores_nothing(cache, store):
    calls = []

    async def run(inputs):
        calls.append(inputs)
        return {'outline': Blob(b'outline')}

    step = Step('outline', Version.of(), run, cache=False)
    results = [asyncio.run(cache.async_run(step, {'x': 1})) for _ in range(2)]
    assert len(calls) == 2
    assert [(r.hit, r.key, r.outputs) for r in results] == [(False, None, {'outline': Blob(b'outline')})] * 2
    assert not store.has(sha(b'outline'))


def test_run_many_returns_results_in_order_and_keeps_each_error_in_its_result(cache):
    err = RuntimeError('model timed out')
    answers = {1: {'y': 10}, 2: err, 3: Refused('blank page'), 4: {'png': Blob(b'png 4')}}
    batches = []

    def run_batch(inputs_list):
        batches.append([inputs['x'] for inputs in inputs_list])
        return [answers[inputs['x']] for inputs in inputs_list]

    step = Step('s', Version.of(), double, run_batch=run_batch)
    first = cache.run_many(step, [{'x': x} for x in (1, 2, 3, 4)])
    assert [r.status for r in first] == ['ok', 'error', 'refused', 'ok']
    assert first[0].outputs == {'y': 10} and first[3].outputs['png'].read() == b'png 4'
    assert first[1].error is err and (first[1].outputs, first[1].result_sha) == ({}, None)
    assert first[2].refusal == Refused('blank page')
    answers[2] = {'y': 20}
    second = cache.run_many(step, [{'x': x} for x in (4, 3, 2, 1)])
    assert batches == [[1, 2, 3, 4], [2]]
    assert [(r.hit, r.status) for r in second] == [(True, 'ok'), (True, 'refused'), (False, 'ok'), (True, 'ok')]
    assert second[2].outputs == {'y': 20}


def test_run_many_computes_identical_inputs_once(cache):
    batches = []

    def run_batch(inputs_list):
        batches.append(inputs_list)
        return [{'token': secrets.token_hex(8)} for _ in inputs_list]

    step = Step('s', Version.of(), double, run_batch=run_batch)
    a, b, again = cache.run_many(step, [{'x': 1}, {'x': 2}, {'x': 1}])
    assert batches == [[{'x': 1}, {'x': 2}]]
    assert again == a and a.result_sha != b.result_sha


def test_when_run_batch_raises_every_miss_it_was_given_gets_the_error(cache):
    err = RuntimeError('renderer crashed')

    def run_batch(inputs_list):
        raise err

    step = Step('s', Version.of(), double, run_batch=run_batch)
    cache.run(step, {'x': 1})
    results = cache.run_many(step, [{'x': 1}, {'x': 2}, {'x': 3}])
    assert [(r.hit, r.status, r.error) for r in results] == [(True, 'ok', None), (False, 'error', err), (False, 'error', err)]


def test_a_run_batch_that_returns_the_wrong_number_of_items_is_an_error(cache):
    answers = iter([[{'y': 1}], [{'y': 1}, {'y': 2}]])
    step = Step('s', Version.of(), double, run_batch=lambda inputs_list: next(answers))
    with pytest.raises(ValueError):
        cache.run_many(step, [{'x': 1}, {'x': 2}])
    results = in_thread(cache.run_many, step, [{'x': 1}, {'x': 2}]).result(10)
    assert [r.outputs for r in results] == [{'y': 1}, {'y': 2}]


def test_run_many_without_run_batch_calls_run_once_per_miss(cache):
    def answer(inputs):
        if inputs['x'] == 2:
            raise RuntimeError('model timed out')
        return double(inputs)

    run, calls = counted(answer)
    results = cache.run_many(Step('s', Version.of(), run), [{'x': 1}, {'x': 2}, {'x': 1}])
    assert calls == [{'x': 1}, {'x': 2}]
    assert [(r.status, r.outputs) for r in results] == [('ok', {'y': 2}), ('error', {}), ('ok', {'y': 2})]
    assert isinstance(results[1].error, RuntimeError)


def test_run_many_runs_a_cache_false_step_on_every_input_and_stores_nothing(cache, store):
    batches = []

    def run_batch(inputs_list):
        batches.append(inputs_list)
        return [{'outline': Blob(b'outline')}, RuntimeError('bad docx')]

    step = Step('outline', Version.of(), double, run_batch=run_batch, cache=False)
    results = cache.run_many(step, [{'x': 1}, {'x': 1}])
    assert batches == [[{'x': 1}, {'x': 1}]]
    assert [(r.status, r.key) for r in results] == [('ok', None), ('error', None)]
    assert not store.has(sha(b'outline'))


@pytest.mark.parametrize(
    'holder_raises, runs, hit', [(False, 1, True), (True, 2, False)], ids=['holder-stores', 'holder-raises']
)
def test_run_many_waits_after_its_batch_for_a_miss_another_worker_holds(cache, monkeypatch, holder_raises, runs, hit):
    monkeypatch.setattr('stepcache.cache.WAIT_SECONDS', 0.01)
    holding, batch_ran = threading.Event(), threading.Event()
    run_calls, batches = [], []

    def run(inputs):
        run_calls.append(inputs)
        if len(run_calls) == 1:
            holding.set()
            batch_ran.wait(10)
            if holder_raises:
                raise RuntimeError('model timed out')
        return {'by': 'run'}

    def run_batch(inputs_list):
        batches.append(inputs_list)
        batch_ran.set()
        return [{'by': 'run_batch'} for _ in inputs_list]

    step = Step('s', Version.of(), run, run_batch=run_batch)
    holder = in_thread(cache.run, step, {'x': 1})
    holding.wait(10)
    held, free = in_thread(cache.run_many, step, [{'x': 1}, {'x': 2}]).result(10)
    assert isinstance(holder.exception(10), RuntimeError) == holder_raises
    assert batches == [[{'x': 2}]] and free.outputs == {'by': 'run_batch'}
    assert (held.hit, held.status, held.outputs, len(run_calls)) == (hit, 'ok', {'by': 'run'}, runs)
