# stepcache

Content-addressed cache for multi-step processing pipelines, with early cutoff.

Each step's result is keyed by the content hashes of its inputs and the identity of
the step (its code, tools and settings). When a step changes, only the items whose
inputs or step identity changed run again, and items whose output comes out
byte-identical stop the re-run there: later steps keep their cached results.

Status: 0.1.0. The API may still change before 1.0.

## Install

stepcache is not on PyPI yet. Install it from this repository. This needs `git` on the
machine, which slim container images such as `python:3.12-slim` do not have.

```
pip install git+https://github.com/karlla1220/stepcache
```

stepcache has no dependencies outside the standard library.

## Example

This pipeline has two steps. `render` turns a text into a page `Blob`, and `count` reads the
page and counts its words. Each document is a subject, so the cache can say later which
documents to process again. The same code is in
[examples/quickstart.py](examples/quickstart.py), and the comments show what it prints.

```python
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
```

## Keys and versions

`Step(name, version, run, run_batch=None, cache=True)` has a name, a `Version` and a `run`
function. `run` takes a dict of inputs and returns a dict of outputs or a `Refused`. Each
input and output value is a `Blob` or a JSON value. A JSON output can be at most 4096
bytes, so return longer data as a `Blob`. A longer one makes `StepCache.run` raise
`ValueError`, and nothing is stored. A `Result` of a stored or reused run has
`status == 'ok'`.

The key of a run is the sha256 of the step name, the version's sha and the inputs. A `Blob`
enters the key by its sha256 and a JSON value as it is. File names and paths do not enter
the key. When a step's output has the same bytes as before, the next step gets the same
input and its key does not change. That is the early cutoff.

`Version.of(files=None, tools=None, rev='0', code=True)` builds the version from four parts:

- `code` is the sha256 of the module file that defines `run`. When `run_batch` is in another
  file, it is a hash over both files. `Step` adds it. Other modules that those files import
  are not in it.
- `files` maps relative paths to sha256 values. `Version.files(root, patterns)` hashes the
  files under `root` that match the glob patterns.
- `tools` maps names to strings that identify external programs or models, such as a version
  string or a binary's hash. The application chooses the strings.
- `rev` is a string that you change by hand for a change that the other parts do not show.

`code=False` leaves the code out. Use it when `run` has no source file, such as a function
defined in an interactive shell. `Step` raises `ValueError` in that case. When any part
changes, every key of the step changes.

## Refusals and errors

A step returns `Refused(reason, detail=None)` when it cannot process an input and the same
input will always get the same answer. The cache stores a refusal like a result, and the
`Result` has `status == 'refused'` and the refusal in `refusal`. `StepCache.run` does not
raise for a refusal. A step raises an exception for an error. The cache stores nothing, the
exception propagates, and the next run tries again.

## `run_many` and `async_run`

`StepCache.run_many(step, inputs_list)` returns one `Result` per inputs dict, in input order.
Hits come from the cache. The misses run in one call of the step's `run_batch`, or one
`run` call each when the step has no `run_batch`. Identical inputs run once. A miss whose
step raised, or whose `run_batch` item is an exception, gets a result with
`status == 'error'` and the exception in `error`. The cache stores nothing for it.

`run_batch(inputs_list)` takes a list of inputs dicts and returns a list of the same length,
in the same order. Each item is a dict of outputs, a `Refused`, or an exception. When
`run_batch` itself raises, every item it was given gets an `'error'` result. When it returns
a list of another length, `run_many` raises `ValueError`.

`StepCache.async_run(step, inputs)` is `run` for async code. It awaits an `async def` step
and calls a sync step directly. An async step runs only through `async_run`, and `run`
raises `TypeError` for one.

## Manifests and stale subjects

A subject is a unit that the application processes, such as one document. Inside
`with cache.subject(name, source=None) as s:`, the calls `s.run`, `s.run_many` and
`s.async_run` work like the `StepCache` methods and record each result in the subject's
manifest. When the block ends normally, the new manifest replaces the old one. When the
block raises, the old manifest stays. `cache.manifest(name)` returns the latest manifest,
or `None`. The manifest has the `source` you passed, so you can compare it with the current
source.

`cache.stale_subjects(pipeline, complete=False)` compares every manifest with the steps in
`pipeline` and returns `(subject, reasons)` pairs, sorted by subject. Each `Reason` has a
`kind`, a `step` and a `detail`:

- `version`: the manifest used another version of the step. `detail['old']` and
  `detail['new']` are the two version shas, and `detail['changed']` lists the parts that
  differ. `changed` is `None` when the old version's parts were never stored.
- `record`: the stored result is gone, for example after `cache.invalidate(step, key)`, or
  another run stored a different result for the key.
- `failed`: the step failed in the block that made the manifest. The failure counts even
  when the block caught the exception and went on. For a cached step, a failure retried
  successfully in the same block is cleared. Any failure of a `cache=False` step in the
  block leaves the subject stale, because a step with no key cannot tell a retry from
  another input.
- `missing_step`: the manifest has no entry for the step. This kind appears only with
  `complete=True`.

A step with `cache=False` runs every time and stores no result. A subject still records its
version, so a change of that version makes the subject stale.

## Concurrency

Several processes on one machine can share one cache. `SqliteStore(path, files)` keeps the
records in a SQLite database at `path` and blobs over 256 KiB in the folder `files`.
Opening a cache made by another schema version raises `SchemaMismatch`, and there is
no migration yet, so delete that cache and let stepcache build a new one.

On a miss, a process claims the key before it runs the step. Other processes that need the
same key wait for the result and do not compute it again. Each claim names a lock file that
the claiming process holds with `flock`. When that process dies, even by `SIGKILL`, the
kernel releases the lock. A waiting process checks the owner every half second, sees
that it is dead, and takes over the claim. stepcache does not release the claim of a process
that is alive but stuck. Stop that process to release its claim.

## Supported environments

- Python 3.12 or later.
- macOS and Linux, and Docker containers on them.
- Windows is not supported, because it has no `flock`.
- Network file systems such as NFS and SMB are not supported, because their file locks are
  not reliable. Put the cache on a local disk.
- Containers can share a cache on one volume or bind mount. On Docker Desktop for Mac, host
  processes and containers must not share one cache. The bind mount does not pass file
  locks between the host and the containers. In a test, the host did not see a lock that a
  container held, and the database became corrupt (`database disk image is malformed`).
  Let one cache be used only by host processes or only by containers.

## Development

Run the tests with [uv](https://docs.astral.sh/uv/):

```
uv run pytest
```

The design and the build plan are in [docs/design.md](docs/design.md) and
[docs/plan.md](docs/plan.md), in Korean.

## License

Apache-2.0. See [LICENSE](LICENSE).
