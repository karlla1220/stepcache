import functools
import glob
import hashlib
import inspect
import json
import os
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path, PurePath
from typing import Any, Callable

KEY_FORMAT = 'stepcache/1'
JSON_OUTPUT_MAX = 4096


def canonical(value) -> bytes:
    # json.dumps would turn a tuple into a list and the key 1 into "1", so two different values could share a key.
    _check_json(value)
    return json.dumps(
        value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False
    ).encode()


def _check_json(value):
    if isinstance(value, dict):
        for k, v in value.items():
            if not isinstance(k, str):
                raise ValueError(f'JSON object keys must be str, not {k!r}')
            _check_json(v)
    elif isinstance(value, list):
        for v in value:
            _check_json(v)
    elif value is not None and not isinstance(value, (str, int, float)):
        raise ValueError(f'{type(value).__name__} is not a JSON value')


def digest(value) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def encode(values: dict) -> dict:
    return {
        name: ['blob', v.sha256] if isinstance(v, Blob) else ['json', v]
        for name, v in values.items()
    }


def check_outputs(outputs) -> dict:
    if not isinstance(outputs, dict):
        raise TypeError(f'a step returns a dict of outputs or Refused, not {type(outputs).__name__}')
    return outputs


def encode_outputs(outputs) -> dict:
    for name, v in check_outputs(outputs).items():
        if not isinstance(v, Blob) and len(canonical(v)) > JSON_OUTPUT_MAX:
            raise ValueError(f'JSON output {name!r} is over {JSON_OUTPUT_MAX} bytes; return a Blob')
    return encode(outputs)


class Blob:
    """Bytes addressed by their sha256. Two blobs are equal when their sha256 is equal."""

    def __init__(self, data: bytes):
        if not isinstance(data, bytes):
            raise TypeError(f'Blob takes bytes, not {type(data).__name__}')
        self.sha256 = hashlib.sha256(data).hexdigest()
        self._data = data
        self._load = None

    @classmethod
    def _stored(cls, sha256: str, load: Callable[[str], bytes]) -> 'Blob':
        blob = cls.__new__(cls)
        blob.sha256, blob._data, blob._load = sha256, None, load
        return blob

    def read(self) -> bytes:
        return self._data if self._load is None else self._load(self.sha256)

    def __eq__(self, other):
        return self.sha256 == other.sha256 if isinstance(other, Blob) else NotImplemented

    def __hash__(self):
        return hash(self.sha256)

    def __repr__(self):
        return f'Blob({self.sha256[:12]})'


@dataclass(frozen=True)
class Refused:
    """A step's deterministic "cannot process this input". It is cached like a result.

    `detail` is a JSON value.
    """

    reason: str
    detail: Any = None


class Version:
    """What a step computes with.

    `components` is {'code': sha | None, 'files': {relpath: sha}, 'tools': {name: str},
    'rev': str}. `sha` hashes it, and keys use that hash.
    """

    def __init__(self, components: dict):
        self.components = components

    @cached_property
    def sha(self) -> str:
        return digest(self.components)

    @classmethod
    def of(cls, files=None, tools=None, rev='0', code=True) -> 'Version':
        """Build a version spec.

        With `code=True`, `Step` puts the hash of the module that defines `run` in it.
        `code=False` leaves the code out.
        """
        return cls({'code': True if code else None, 'files': files or {}, 'tools': tools or {}, 'rev': rev})

    @staticmethod
    def files(root, patterns) -> dict[str, str]:
        """Hash the files under `root` that match the glob patterns.

        Returns {relative path: sha256} sorted by path, with `/` separators, so the same tree
        under another root gives the same result. `**` matches any depth. As in `glob`, a name
        starting with a dot matches only a pattern that spells the dot. A pattern that matches no
        file raises ValueError, so a wrong root cannot leave the files out unnoticed.
        """
        found = {}
        for pattern in patterns:
            paths = [
                p for p in glob.glob(pattern, root_dir=root, recursive=True)
                if os.path.isfile(os.path.join(root, p))
            ]
            if not paths:
                raise ValueError(f'{pattern!r} matches no file under {root}')
            for p in paths:
                found[PurePath(p).as_posix()] = _file_sha(Path(root, p))
        return dict(sorted(found.items()))


class Step:
    """A named computation and the version it computes with.

    `run(inputs) -> outputs | Refused`, where inputs and outputs map names to a Blob or a
    JSON value. An `async def` run is called only through `StepCache.async_run`. A
    `cache=False` step runs every time and stores nothing.

    `valid(outputs) -> bool` says whether a stored 'ok' result can still be used, for outputs
    that name files outside the store. It gets the outputs as a `Result` has them. A record it
    rejects is a miss. It checks state, not identity, so it is not part of the key or version.
    """

    def __init__(self, name: str, version: Version, run, run_batch=None, cache=True, valid=None):
        if version.components['code'] is True:
            version = Version({**version.components, 'code': _code_sha(run, run_batch)})
        self.name = name
        self.version = version
        self.run = run
        self.run_batch = run_batch
        self.cache = cache
        self.valid = valid
        self.is_async = inspect.iscoroutinefunction(run)


def _code_sha(run, run_batch) -> str:
    paths = dict.fromkeys(_source_file(f) for f in (run, run_batch) if f is not None)
    shas = [_file_sha(p) for p in paths]
    return shas[0] if len(shas) == 1 else digest(shas)


def _source_file(fn) -> str:
    target = fn
    while isinstance(target, functools.partial):
        target = target.func
    target = inspect.unwrap(target)
    if not (inspect.isfunction(target) or inspect.ismethod(target) or inspect.isclass(target)):
        target = type(target)
    try:
        path = inspect.getsourcefile(target)
    except (TypeError, OSError):
        path = None
    if path is None or not os.path.isfile(path):
        raise ValueError(
            f'cannot find the source file of {fn!r}; pass Version.of(code=False) '
            'and declare what the step depends on in files, tools or rev'
        )
    return path


def _file_sha(path) -> str:
    with open(path, 'rb') as f:
        return hashlib.file_digest(f, 'sha256').hexdigest()
