import socket
import time
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, version

from .step import KEY_FORMAT, Blob, Refused, Step, check_outputs, digest, encode, encode_outputs
from .store import Record, SqliteStore

try:
    _VERSION = version('stepcache')
except PackageNotFoundError:
    _VERSION = 'unknown'

_PRODUCER = {'host': socket.gethostname(), 'stepcache': _VERSION, 'imported': False}


@dataclass(frozen=True)
class Result:
    """The result of one run.

    `outputs` maps names to a Blob or a JSON value and is empty when the step refused.
    A `cache=False` step's result has no `key` and no `result_sha`.
    """

    hit: bool
    status: str
    outputs: dict
    refusal: Refused | None
    key: str | None
    result_sha: str | None


class StepCache:
    def __init__(self, store: SqliteStore):
        self.store = store

    def run(self, step: Step, inputs: dict) -> Result:
        """Return the stored result for these inputs, or run the step and store its result.

        A step that raises stores nothing, and the exception propagates. When another writer
        stored a result for the key first, that result is returned.
        """
        if not step.cache:
            out = step.run(inputs)
            if isinstance(out, Refused):
                return Result(False, 'refused', {}, out, None, None)
            return Result(False, 'ok', check_outputs(out), None, None, None)

        encoded_inputs = encode(inputs)
        key = digest([KEY_FORMAT, step.name, step.version.sha, encoded_inputs])
        record = self.store.get_record(key)
        if record is not None:
            shas = [sha for kind, sha in record.outputs.values() if kind == 'blob']
            if all(map(self.store.has, shas)):
                self.store.touch_record(key)
                for sha in shas:
                    self.store.touch(sha)
                return self._result(record, hit=True)
            self.store.delete_record(key)

        started = time.monotonic()
        out = step.run(inputs)
        duration_ms = round((time.monotonic() - started) * 1000)
        if isinstance(out, Refused):
            status, outputs, refusal = 'refused', {}, {'reason': out.reason, 'detail': out.detail}
        else:
            status, outputs, refusal = 'ok', encode_outputs(out), None
        result_sha = digest([status, outputs, refusal])
        if status == 'ok':
            for value in out.values():
                if isinstance(value, Blob):
                    self.store.put(value.read())
        self.store.put_version(step.version.sha, step.version.components)
        record = self.store.put_record_if_absent(Record(
            key=key,
            step=step.name,
            version_sha=step.version.sha,
            inputs=encoded_inputs,
            status=status,
            outputs=outputs,
            refusal=refusal,
            result_sha=result_sha,
            duration_ms=duration_ms,
            producer=_PRODUCER,
            input_key=digest([KEY_FORMAT, step.name, encoded_inputs]),
        ))
        return self._result(record, hit=False)

    def invalidate(self, step: Step, key: str):
        """Delete the record of `key`, so the next run computes it again."""
        self.store.delete_record(key)

    def _result(self, record: Record, hit: bool) -> Result:
        outputs = {
            name: Blob._stored(value, self.store.get) if kind == 'blob' else value
            for name, (kind, value) in record.outputs.items()
        }
        refusal = None if record.refusal is None else Refused(**record.refusal)
        return Result(hit, record.status, outputs, refusal, record.key, record.result_sha)
