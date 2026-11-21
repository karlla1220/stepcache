import tempfile
from pathlib import Path

from stepcache import Blob, Refused, SqliteStore, Step, StepCache, Version


def render(inputs):
    words = inputs['text'].split()
    if not words:
        return Refused('empty text')
    return {'page': Blob(' '.join(words).encode())}


def count(inputs):
    return {'words': len(inputs['page'].read().split())}


render_v1 = Step('render', Version.of(tools={'renderer': '1.0'}, rev='1'), render)
count_step = Step('count', Version.of(rev='1'), count)


def process(cache, render_step, subject, text):
    with cache.subject(subject) as s:
        page = s.run(render_step, {'text': text})
        if page.status == 'refused':
            return [page]
        return [page, s.run(count_step, {'page': page.outputs['page']})]


with tempfile.TemporaryDirectory() as root:
    cache = StepCache(SqliteStore(Path(root, 'stepcache.db'), files=Path(root, 'blobs')))

    for attempt in (1, 2):
        results = process(cache, render_v1, 'doc:hello', 'hello   world')
        print(attempt, [r.hit for r in results], results[-1].outputs)
    # 1 [False, False] {'words': 2}
    # 2 [True, True] {'words': 2}

    empty = process(cache, render_v1, 'doc:empty', '   ')
    print(empty[0].status, empty[0].refusal.reason)
    # refused empty text

    # A new renderer makes every subject rendered with the old one stale.
    render_v2 = Step('render', Version.of(tools={'renderer': '1.1'}, rev='1'), render)
    pipeline = [render_v2, count_step]
    for subject, reasons in cache.stale_subjects(pipeline):
        print(subject, [(r.kind, r.step, r.detail['changed']) for r in reasons])
    # doc:empty [('version', 'render', {'tools': {'added': [], 'removed': [], 'changed': ['renderer']}})]
    # doc:hello [('version', 'render', {'tools': {'added': [], 'removed': [], 'changed': ['renderer']}})]

    # render runs again. Its page has the same bytes, so count is a hit.
    results = process(cache, render_v2, 'doc:hello', 'hello   world')
    print([r.hit for r in results])
    # [False, True]

    process(cache, render_v2, 'doc:empty', '   ')
    print(cache.stale_subjects(pipeline))
    # []
