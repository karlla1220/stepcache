import hashlib
import json
import os
import secrets
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, fields, replace
from datetime import datetime, timezone
from pathlib import Path

SCHEMA_VERSION = 1
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
        result_sha TEXT
    )""",
    'CREATE INDEX manifest_entries_subject ON manifest_entries (subject)',
    'CREATE TABLE claims (key TEXT PRIMARY KEY, owner TEXT NOT NULL)',
]

# Connections inherited across fork. SQLite must not touch them in the child, not even to close.
_inherited = []


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


class SqliteStore:
    """A store on one machine's local disk: a SQLite database and a folder for large blobs.

    Blobs up to INLINE_BLOB_MAX bytes live in the database, larger ones in
    `files/<sha[:2]>/<sha[2:]>`. Processes on the same machine may share the store.
    """

    def __init__(self, path, files):
        self.path = Path(path)
        self.files = Path(files)
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
        where = f'WHERE {column} = ? AND last_used_at < ?'
        args = (value, stamp[:10])
        if self._conn().execute(f'SELECT 1 FROM {table} {where}', args).fetchone():
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

    def delete_record(self, key: str):
        with self._write() as c:
            c.execute('DELETE FROM records WHERE key = ?', (key,))

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
