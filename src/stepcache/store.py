import atexit
import fcntl
import hashlib
import json
import os
import re
import secrets
import sqlite3
import threading
import time
from contextlib import contextmanager, suppress
from dataclasses import dataclass, fields, replace
from datetime import datetime, timezone
from pathlib import Path

SCHEMA_VERSION = 2
INLINE_BLOB_MAX = 256 * 1024
BUSY_TIMEOUT_SECONDS = 60

_SCHEMA = [
    'CREATE TABLE meta (name TEXT PRIMARY KEY, value NOT NULL)',
    'CREATE TABLE versions (version_sha TEXT PRIMARY KEY, components TEXT NOT NULL)',
    """CREATE TABLE records (
        key TEXT PRIMARY KEY,
        step TEXT NOT NULL,
        version_sha TEXT NOT NULL,
        inputs TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('ok', 'refused')),
        outputs TEXT NOT NULL,
        refusal TEXT,
        result_sha TEXT NOT NULL,
        duration_ms INTEGER NOT NULL,
        producer TEXT NOT NULL,
        input_key TEXT NOT NULL,
        created_at TEXT NOT NULL,
        last_used_at TEXT NOT NULL
    )""",
    'CREATE INDEX records_input_key ON records (input_key)',
    """CREATE TABLE blobs (
        sha TEXT PRIMARY KEY,
        size INTEGER NOT NULL,
        last_used_at TEXT NOT NULL,
        data BLOB
    )""",
    'CREATE TABLE manifests (subject TEXT PRIMARY KEY, produced_at TEXT NOT NULL, source TEXT)',
    """CREATE TABLE manifest_entries (
        subject TEXT NOT NULL,
        step TEXT NOT NULL,
        version_sha TEXT NOT NULL,
        key TEXT,
        result_sha TEXT,
        failed INTEGER NOT NULL DEFAULT 0 CHECK (failed IN (0, 1))
    )""",
    'CREATE INDEX manifest_entries_subject ON manifest_entries (subject)',
    'CREATE TABLE claims (key TEXT PRIMARY KEY, owner TEXT NOT NULL)',
]

# Connections inherited across fork. SQLite must not touch them in the child, not even to close.
_inherited = []

_LOCK_NAME = re.compile('[0-9a-f]{32}')

_held_locks: dict[Path, tuple[int, str]] = {}
_held_locks_guard = threading.Lock()


def _drop_inherited_locks():
    # An inherited descriptor shares the parent's lock, so a child that never touches stepcache
    # would keep a dead parent's claims looking alive. Closing it leaves the parent's lock held.
    global _held_locks_guard
    _held_locks_guard = threading.Lock()
    for fd, _ in _held_locks.values():
        os.close(fd)
    _held_locks.clear()


os.register_at_fork(after_in_child=_drop_inherited_locks)


@atexit.register
def _remove_held_locks():
    for folder, (_, name) in _held_locks.items():
        (folder / name).unlink(missing_ok=True)


class SchemaMismatch(Exception):
    """The database was not made by this schema version of stepcache."""


def now() -> datetime:
    return datetime.now(timezone.utc)


def _stamp() -> str:
    return now().isoformat(timespec='microseconds')


@dataclass(frozen=True)
class Record:
    """The stored result of one key.

    `inputs` and `outputs` map names to ['blob', sha] or ['json', value]. The store sets
    `created_at` and `last_used_at` when it inserts the record.
    """

    key: str
    step: str
    version_sha: str
    inputs: dict
    status: str
    outputs: dict
    refusal: dict | None
    result_sha: str
    duration_ms: int
    producer: dict
    input_key: str
    created_at: str | None = None
    last_used_at: str | None = None


@dataclass(frozen=True)
class Manifest:
    """The latest manifest of one subject.

    `produced_at` is when it was stored, as an ISO 8601 UTC time. `source` is what the
    application passed to `subject`. Each entry is (step, version_sha, key, result_sha, failed).
    """

    subject: str
    produced_at: str
    source: str | None
    entries: frozenset


_RECORD_COLUMNS = [f.name for f in fields(Record)]
_RECORD_JSON = {'inputs', 'outputs', 'refusal', 'producer'}
_SELECT_RECORD = f'SELECT {", ".join(_RECORD_COLUMNS)} FROM records WHERE key = ?'
_INSERT_RECORD = (
    f'INSERT INTO records ({", ".join(_RECORD_COLUMNS)}) '
    f'VALUES ({", ".join("?" * len(_RECORD_COLUMNS))}) ON CONFLICT (key) DO NOTHING'
)


def _to_column(name: str, value):
    return json.dumps(value, ensure_ascii=False) if name in _RECORD_JSON and value is not None else value


def _from_column(name: str, value):
    return json.loads(value) if name in _RECORD_JSON and value is not None else value


def _record_row(record: Record) -> list:
    return [_to_column(name, getattr(record, name)) for name in _RECORD_COLUMNS]


def _row_record(row) -> Record:
    return Record(*map(_from_column, _RECORD_COLUMNS, row))


_STALE = """
WITH pipeline (position, step, version_sha) AS (
    SELECT key, json_extract(value, '$[0]'), json_extract(value, '$[1]') FROM json_each(?)
), reasons (subject, position, kind_order, kind, step, value, components) AS (
    SELECT DISTINCT e.subject, p.position, 0, 'version', e.step, e.version_sha, v.components
    FROM manifest_entries e
    JOIN pipeline p ON p.step = e.step AND p.version_sha != e.version_sha
    LEFT JOIN versions v ON v.version_sha = e.version_sha
    UNION ALL
    SELECT DISTINCT e.subject, p.position, 1, 'record', e.step, e.key, NULL
    FROM manifest_entries e
    JOIN pipeline p ON p.step = e.step
    LEFT JOIN records r ON r.key = e.key
    WHERE e.key IS NOT NULL AND NOT e.failed AND r.result_sha IS NOT e.result_sha
    UNION ALL
    SELECT DISTINCT e.subject, p.position, 2, 'failed', e.step, e.key, NULL
    FROM manifest_entries e
    JOIN pipeline p ON p.step = e.step
    WHERE e.failed
    UNION ALL
    SELECT m.subject, p.position, 3, 'missing_step', p.step, NULL, NULL
    FROM manifests m CROSS JOIN pipeline p
    WHERE ? AND NOT EXISTS (SELECT 1 FROM manifest_entries e WHERE e.subject = m.subject AND e.step = p.step)
)
SELECT subject, kind, step, value, components FROM reasons ORDER BY subject, position, kind_order, value
"""


class SqliteStore:
    """A store on one machine's local disk: a SQLite database and a folder for large blobs.

    Blobs up to INLINE_BLOB_MAX bytes live in the database, larger ones in
    `files/<sha[:2]>/<sha[2:]>`. Processes on the same machine may share the store.

    A claim names its owner by a lock file in `<db file name>.locks/` that the claiming process
    holds with `flock` until it exits. An owner whose file is gone or can be locked is dead, and
    so is an owner that is not a lock file name: the database may be written by other users, so
    its owner values never become other paths.
    """

    def __init__(self, path, files):
        self.path = Path(path)
        self.files = Path(files)
        self._locks = self.path.with_name(f'{self.path.name}.locks').resolve()
        self._local = threading.local()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._write() as c:
            version = _schema_version(c, self.path)
            if version is None:
                for statement in _SCHEMA:
                    c.execute(statement)
                c.execute("INSERT INTO meta VALUES ('schema_version', ?)", (SCHEMA_VERSION,))
                version = SCHEMA_VERSION
        if version != SCHEMA_VERSION:
            raise SchemaMismatch(f'{self.path} has schema version {version}, not {SCHEMA_VERSION}')

    def _conn(self) -> sqlite3.Connection:
        local = self._local
        if getattr(local, 'pid', None) != os.getpid():
            if hasattr(local, 'conn'):
                _inherited.append(local.conn)
            local.conn = sqlite3.connect(self.path, timeout=BUSY_TIMEOUT_SECONDS, isolation_level=None)
            _use_wal(local.conn)
            local.conn.execute('PRAGMA synchronous = NORMAL')
            local.pid = os.getpid()
        return local.conn

    @contextmanager
    def _write(self):
        c = self._conn()
        with c:
            c.execute('BEGIN IMMEDIATE')
            yield c

    def _touch(self, table: str, column: str, value: str):
        stamp = _stamp()
        today = stamp[:10]
        where = f'WHERE {column} = ? AND last_used_at < ?'
        args = (value, today)
        stale = self._conn().execute(f'SELECT 1 FROM {table} {where}', args).fetchone()
        if stale:
            with self._write() as c:
                c.execute(f'UPDATE {table} SET last_used_at = ? {where}', (stamp, *args))

    def get_record(self, key: str) -> Record | None:
        row = self._conn().execute(_SELECT_RECORD, (key,)).fetchone()
        return None if row is None else _row_record(row)

    def put_record_if_absent(self, record: Record) -> Record:
        """Insert the record unless its key has one, and return the record the store holds."""
        stamp = _stamp()
        record = replace(record, created_at=stamp, last_used_at=stamp)
        with self._write() as c:
            c.execute(_INSERT_RECORD, _record_row(record))
            row = c.execute(_SELECT_RECORD, (record.key,)).fetchone()
        return _row_record(row)

    def delete_record(self, key: str, created_at: str | None = None):
        """Delete the record of `key`, or with `created_at` only the record created then."""
        with self._write() as c:
            c.execute(
                'DELETE FROM records WHERE key = ? AND created_at = coalesce(?, created_at)', (key, created_at)
            )

    def touch_record(self, key: str):
        """Mark the record used now, writing at most once per UTC day."""
        self._touch('records', 'key', key)

    def _blob_path(self, sha: str) -> Path:
        return self.files / sha[:2] / sha[2:]

    def put(self, data: bytes) -> str:
        sha = hashlib.sha256(data).hexdigest()
        if self.has(sha):
            self.touch(sha)
            return sha
        inline = len(data) <= INLINE_BLOB_MAX
        if not inline:
            self._write_file(sha, data)
        with self._write() as c:
            c.execute(
                'INSERT INTO blobs (sha, size, last_used_at, data) VALUES (?, ?, ?, ?) '
                'ON CONFLICT (sha) DO NOTHING',
                (sha, len(data), _stamp(), data if inline else None),
            )
        return sha

    def _write_file(self, sha: str, data: bytes):
        path = self._blob_path(sha)
        path.parent.mkdir(parents=True, exist_ok=True)
        # Not tempfile.mkstemp: its 0600 mode would hide the blob from other users of the store.
        tmp = path.with_name(f'{path.name}.{secrets.token_hex(8)}.tmp')
        try:
            with open(tmp, 'xb') as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise

    def get(self, sha: str) -> bytes:
        row = self._conn().execute('SELECT data FROM blobs WHERE sha = ?', (sha,)).fetchone()
        if row is None:
            raise KeyError(sha)
        return self._blob_path(sha).read_bytes() if row[0] is None else row[0]

    def has(self, sha: str) -> bool:
        row = self._conn().execute('SELECT data IS NULL FROM blobs WHERE sha = ?', (sha,)).fetchone()
        return row is not None and (not row[0] or self._blob_path(sha).exists())

    def touch(self, sha: str):
        """Mark the blob used now, writing at most once per UTC day."""
        self._touch('blobs', 'sha', sha)

    def delete(self, sha: str):
        with self._write() as c:
            c.execute('DELETE FROM blobs WHERE sha = ?', (sha,))
        self._blob_path(sha).unlink(missing_ok=True)

    def claim(self, key: str) -> bool:
        """Claim `key` for this process unless it is claimed, and say whether this call claimed it."""
        owner = self._lock_name()
        with self._write() as c:
            return c.execute(
                'INSERT INTO claims (key, owner) VALUES (?, ?) ON CONFLICT (key) DO NOTHING', (key, owner)
            ).rowcount == 1

    def claim_owner(self, key: str) -> str | None:
        row = self._conn().execute('SELECT owner FROM claims WHERE key = ?', (key,)).fetchone()
        return None if row is None else row[0]

    def owner_alive(self, owner: str) -> bool:
        """Whether the process that holds the lock file named `owner` is alive.

        A dead owner's lock file is deleted.
        """
        held = _held_locks.get(self._locks)
        if held is not None and held[1] == owner:
            return True
        if not _LOCK_NAME.fullmatch(owner):
            return False
        path = self._locks / owner
        try:
            fd = os.open(path, os.O_RDONLY)
        except FileNotFoundError:
            return False
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        else:
            # Deleting is tidying only. Another user's file in a shared folder may not be ours to delete.
            with suppress(OSError):
                path.unlink()
            return False
        finally:
            os.close(fd)

    def take_over(self, key: str, dead_owner: str) -> bool:
        """Move the claim on `key` from `dead_owner` to this process, and say whether it moved."""
        owner = self._lock_name()
        with self._write() as c:
            return c.execute(
                'UPDATE claims SET owner = ? WHERE key = ? AND owner = ?', (owner, key, dead_owner)
            ).rowcount == 1

    def release(self, key: str):
        """Delete this process's claim on `key`."""
        owner = self._lock_name()
        with self._write() as c:
            c.execute('DELETE FROM claims WHERE key = ? AND owner = ?', (key, owner))

    def _lock_name(self) -> str:
        with _held_locks_guard:
            if self._locks not in _held_locks:
                self._locks.mkdir(exist_ok=True)
                name = secrets.token_hex(16)
                # The file gets a lock file name only once it is locked, so a sweep never deletes it.
                tmp = self._locks / f'{name}.tmp'
                fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o666)
                fcntl.flock(fd, fcntl.LOCK_EX)
                os.rename(tmp, self._locks / name)
                _held_locks[self._locks] = (fd, name)
                for path in self._locks.iterdir():
                    self.owner_alive(path.name)
            return _held_locks[self._locks][1]

    def put_version(self, sha: str, components: dict):
        with self._write() as c:
            c.execute(
                'INSERT INTO versions (version_sha, components) VALUES (?, ?) '
                'ON CONFLICT (version_sha) DO NOTHING',
                (sha, json.dumps(components, ensure_ascii=False)),
            )

    def get_version(self, sha: str) -> dict | None:
        row = self._conn().execute(
            'SELECT components FROM versions WHERE version_sha = ?', (sha,)
        ).fetchone()
        return None if row is None else json.loads(row[0])

    def put_manifest(self, subject: str, source: str | None, entries):
        """Replace the manifest of `subject`. Each entry is (step, version_sha, key, result_sha, failed)."""
        with self._write() as c:
            c.execute(
                'INSERT INTO manifests (subject, produced_at, source) VALUES (?, ?, ?) ON CONFLICT (subject) '
                'DO UPDATE SET produced_at = excluded.produced_at, source = excluded.source',
                (subject, _stamp(), source),
            )
            c.execute('DELETE FROM manifest_entries WHERE subject = ?', (subject,))
            c.executemany(
                'INSERT INTO manifest_entries (subject, step, version_sha, key, result_sha, failed) '
                'VALUES (?, ?, ?, ?, ?, ?)',
                [(subject, *entry) for entry in entries],
            )

    def get_manifest(self, subject: str) -> Manifest | None:
        rows = self._conn().execute(
            'SELECT m.produced_at, m.source, e.step, e.version_sha, e.key, e.result_sha, e.failed FROM manifests m '
            'LEFT JOIN manifest_entries e ON e.subject = m.subject WHERE m.subject = ?',
            (subject,),
        ).fetchall()
        if not rows:
            return None
        entries = frozenset((*row[2:6], bool(row[6])) for row in rows if row[2] is not None)
        return Manifest(subject, rows[0][0], rows[0][1], entries)

    def stale_reasons(self, pipeline: dict[str, str], complete: bool) -> list[tuple]:
        """Compare every manifest with `pipeline`, which maps step names to their current version_sha.

        Returns (subject, kind, step, value, components) rows, sorted by subject, then pipeline
        order, then kind, then value. Entries of steps not in `pipeline` are ignored.

        - 'version': one row per subject, step and old version_sha. `value` is the old version_sha
          and `components` its stored components, or None when `versions` lacks it.
        - 'record': one row per subject and key of an entry that did not fail and whose record is
          gone or has another result_sha. `value` is the key.
        - 'failed': one row per subject, step and key of a failed entry. `value` is the key, which
          may be None.
        - 'missing_step': only with `complete`, a pipeline step the manifest has no entry for.
        """
        rows = self._conn().execute(_STALE, (json.dumps(list(pipeline.items())), complete)).fetchall()
        return [
            (subject, kind, step, value, None if components is None else json.loads(components))
            for subject, kind, step, value, components in rows
        ]


def _use_wal(c: sqlite3.Connection):
    # Switching a new database to WAL needs an exclusive lock, and SQLite reports SQLITE_BUSY
    # at once instead of waiting on busy_timeout when other processes are opening it too.
    deadline = time.monotonic() + BUSY_TIMEOUT_SECONDS
    while True:
        try:
            c.execute('PRAGMA journal_mode = WAL')
            return
        except sqlite3.OperationalError as e:
            if e.sqlite_errorcode & 0xff != sqlite3.SQLITE_BUSY or time.monotonic() > deadline:
                raise
            time.sleep(0.01)


def _schema_version(c: sqlite3.Connection, path: Path) -> int | None:
    tables = {name for (name,) in c.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    if not tables:
        return None
    row = c.execute("SELECT value FROM meta WHERE name = 'schema_version'").fetchone() if 'meta' in tables else None
    if row is None:
        raise SchemaMismatch(f'{path} is not a stepcache database')
    return row[0]
