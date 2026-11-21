import asyncio
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

WAIT_SECONDS = 0.5


@dataclass(frozen=True)
class Result:
    """The result of one run.

    `hit` is False only when this call ran the step. `status` is 'ok', 'refused', or, only in a
    `run_many` result, 'error' with the exception in `error`. `outputs` maps names to a Blob or
    a JSON value and is empty unless the status is 'ok'. A `cache=False` step's result has no
    `key`, and only an 'ok' or 'refused' result of a cached step has a `result_sha`.
    """

    hit: bool
    status: str
    outputs: dict
    refusal: Refused | None
    key: str | None
    result_sha: str | None
    error: BaseException | None = None


class StepCache:
    def __init__(self, store: SqliteStore):
        self.store = store

    def run(self, step: Step, inputs: dict) -> Result:
        """Return the stored result for these inputs, or run the step and store its result.

        While another worker computes the same key, this waits for its result. A step that raises
        stores nothing, and the exception propagates. When another writer stored a result for the
        key first, that result is returned.
        """
        _check_sync(step)
        if not step.cache:
            return _uncached(step.run(inputs))
        key, encoded_inputs = _key(step, inputs)
        record = _wait(self._record_or_claim(key))
        if record is not None:
            return self._result(record, hit=True)
        return self._compute(step, key, encoded_inputs, inputs)

    async def async_run(self, step: Step, inputs: dict) -> Result:
        """`run` for async code. It awaits an async step and calls a sync one directly.

        While another worker computes the same key, this waits without blocking the event loop.
        """
        if not step.cache:
            out = step.run(inputs)
            return _uncached(await out if step.is_async else out)
        key, encoded_inputs = _key(step, inputs)
        record = await _async_wait(self._record_or_claim(key))
        if record is not None:
            return self._result(record, hit=True)
        try:
            started = time.monotonic()
            out = step.run(inputs)
            if step.is_async:
                out = await out
            return self._write(step, key, encoded_inputs, out, _ms_since(started))
        finally:
            self.store.release(key)

    def run_many(self, step: Step, inputs_list: list[dict]) -> list[Result]:
        """Run the step on each inputs dict and return the results in input order.

        Hits are filled at once. The misses this caller claims run in one `run_batch` call, or in
        one `run` call each when the step has no `run_batch`, and identical inputs run once.
        Misses that another worker holds are waited for after that. A miss whose step raised, or
        whose `run_batch` item is an exception, stores nothing and gets an 'error' result.
        """
        _check_sync(step)
        if not step.cache:
            return [_settle(None, out, _uncached) for out in _batch(step, inputs_list)]
        keys = []
        distinct = {}
        for inputs in inputs_list:
            key, encoded_inputs = _key(step, inputs)
            keys.append(key)
            distinct.setdefault(key, (encoded_inputs, inputs))
        results, claimed, waiting = {}, [], {}
        try:
            for key in distinct:
                protocol = self._record_or_claim(key)
                try:
                    next(protocol)
                    waiting[key] = protocol
                except StopIteration as done:
                    if done.value is None:
                        claimed.append(key)
                    else:
                        results[key] = self._result(done.value, hit=True)
            if claimed:
                started = time.monotonic()
                outs = _batch(step, [distinct[key][1] for key in claimed])
                duration_ms = round(_ms_since(started) / len(claimed))
                for key, out in zip(claimed, outs):
                    results[key] = _settle(
                        key, out, lambda out: self._write(step, key, distinct[key][0], out, duration_ms)
                    )
        finally:
            for key in claimed:
                self.store.release(key)
        for key, protocol in waiting.items():
            record = _wait(protocol)
            if record is not None:
                results[key] = self._result(record, hit=True)
                continue
            try:
                results[key] = self._compute(step, key, *distinct[key])
            except Exception as e:
                results[key] = _failed(key, e)
        return [results[key] for key in keys]

    def invalidate(self, step: Step, key: str):
        """Delete the record of `key`, so the next run computes it again."""
        self.store.delete_record(key)

    def _record_or_claim(self, key: str):
        held = False
        while True:
            try:
                record = self._lookup(key)
            except BaseException:
                if held:
                    self.store.release(key)
                raise
            if held:
                if record is not None:
                    self.store.release(key)
                return record
            if record is not None:
                return record
            owner = self.store.claim_owner(key)
            if owner is None:
                held = self.store.claim(key)
            elif not self.store.owner_alive(owner):
                held = self.store.take_over(key, owner)
            if not held:
                yield

    def _lookup(self, key: str) -> Record | None:
        record = self.store.get_record(key)
        if record is None:
            return None
        shas = [sha for kind, sha in record.outputs.values() if kind == 'blob']
        if not all(map(self.store.has, shas)):
            self.store.delete_record(key)
            return None
        self.store.touch_record(key)
        for sha in shas:
            self.store.touch(sha)
        return record

    def _compute(self, step: Step, key: str, encoded_inputs: dict, inputs: dict) -> Result:
        try:
            started = time.monotonic()
            out = step.run(inputs)
            return self._write(step, key, encoded_inputs, out, _ms_since(started))
        finally:
            self.store.release(key)

    def _write(self, step: Step, key: str, encoded_inputs: dict, out, duration_ms: int) -> Result:
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

    def _result(self, record: Record, hit: bool) -> Result:
        outputs = {
            name: Blob._stored(value, self.store.get) if kind == 'blob' else value
            for name, (kind, value) in record.outputs.items()
        }
        refusal = None if record.refusal is None else Refused(**record.refusal)
        return Result(hit, record.status, outputs, refusal, record.key, record.result_sha)


def _key(step: Step, inputs: dict) -> tuple[str, dict]:
    encoded_inputs = encode(inputs)
    return digest([KEY_FORMAT, step.name, step.version.sha, encoded_inputs]), encoded_inputs


def _check_sync(step: Step):
    if step.is_async:
        raise TypeError(f'step {step.name!r} is async; call it with async_run')


def _wait(protocol):
    while True:
        try:
            next(protocol)
        except StopIteration as done:
            return done.value
        time.sleep(WAIT_SECONDS)


async def _async_wait(protocol):
    while True:
        try:
            next(protocol)
        except StopIteration as done:
            return done.value
        await asyncio.sleep(WAIT_SECONDS)


def _batch(step: Step, inputs_list: list[dict]) -> list:
    if step.run_batch is None or not inputs_list:
        return [_call(step.run, inputs) for inputs in inputs_list]
    try:
        outs = step.run_batch(inputs_list)
    except Exception as e:
        return [e] * len(inputs_list)
    if len(outs) != len(inputs_list):
        raise ValueError(
            f'run_batch of step {step.name!r} returned {len(outs)} items for {len(inputs_list)} inputs'
        )
    return outs


def _call(run, inputs):
    try:
        return run(inputs)
    except Exception as e:
        return e


def _settle(key: str | None, out, finish) -> Result:
    if not isinstance(out, Exception):
        try:
            return finish(out)
        except Exception as e:
            out = e
    return _failed(key, out)


def _failed(key: str | None, error: Exception) -> Result:
    return Result(False, 'error', {}, None, key, None, error)


def _uncached(out) -> Result:
    if isinstance(out, Refused):
        return Result(False, 'refused', {}, out, None, None)
    return Result(False, 'ok', check_outputs(out), None, None, None)


def _ms_since(started: float) -> int:
    return round((time.monotonic() - started) * 1000)
