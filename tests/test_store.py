import hashlib
import multiprocessing
import os
import signal
import sqlite3
import subprocess
import sys
import time
from contextlib import closing
from datetime import datetime, timezone

import pytest

from stepcache import SchemaMismatch, SqliteStore
from stepcache.store import Record

KIB = 1024


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def at(day: int, hour: int) -> datetime:
    return datetime(2026, 1, day, hour, tzinfo=timezone.utc)


def blob_last_used(store, blob_sha) -> datetime:
    with closing(sqlite3.connect(store.path)) as c:
        (value,) = c.execute('SELECT last_used_at FROM blobs WHERE sha = ?', (blob_sha,)).fetchone()
    return datetime.fromisoformat(value)


def record(key: str, answer: str) -> Record:
    return Record(
        key=key, step='s', version_sha='v', inputs={'q': ['json', 1]}, status='ok',
        outputs={'answer': ['json', answer]}, refusal=None, result_sha=sha(answer.encode()),
        duration_ms=1, producer={'host': 'h'}, input_key='i',
    )


def test_blobs_up_to_256_kib_stay_in_the_db_and_larger_ones_go_to_files(store):
    small, large = b's' * (256 * KIB), b'l' * (256 * KIB + 1)
    small_sha, large_sha = store.put(small), store.put(large)
    assert small_sha == sha(small) and large_sha == sha(large)
    assert not (store.files / small_sha[:2] / small_sha[2:]).exists()
    assert (store.files / large_sha[:2] / large_sha[2:]).read_bytes() == large
    assert store.get(small_sha) == small
    assert store.get(large_sha) == large


def test_blobs_can_be_checked_and_deleted(store):
    empty, large = store.put(b''), store.put(b'l' * (256 * KIB + 1))
    assert store.get(empty) == b''
    for blob_sha in (empty, large):
        assert store.has(blob_sha)
        store.delete(blob_sha)
        assert not store.has(blob_sha)
        with pytest.raises(KeyError):
            store.get(blob_sha)
    assert not (store.files / large[:2] / large[2:]).exists()


def test_a_large_blob_whose_file_is_gone_is_missing_until_put_again(store):
    data = b'l' * (256 * KIB + 1)
    blob_sha = store.put(data)
    (store.files / blob_sha[:2] / blob_sha[2:]).unlink()
    assert not store.has(blob_sha)
    store.put(data)
    assert store.has(blob_sha) and store.get(blob_sha) == data


def test_blob_last_used_at_is_written_at_most_once_a_day(store, clock):
    blob_sha = store.put(b'png')
    assert blob_last_used(store, blob_sha) == at(1, 9)
    clock.now = at(2, 10)
    store.touch(blob_sha)
    assert blob_last_used(store, blob_sha) == at(2, 10)
    clock.now = at(2, 23)
    store.touch(blob_sha)
    assert blob_last_used(store, blob_sha) == at(2, 10)
    clock.now = at(3, 0)
    assert store.put(b'png') == blob_sha
    assert blob_last_used(store, blob_sha) == at(3, 0)


def test_put_record_if_absent_returns_the_record_already_stored(store, clock):
    first = store.put_record_if_absent(record('k', 'first'))
    assert first.outputs == {'answer': ['json', 'first']}
    assert datetime.fromisoformat(first.created_at) == at(1, 9)
    clock.now = at(1, 10)
    assert store.put_record_if_absent(record('k', 'second')) == first
    assert store.get_record('k') == first


def test_a_deleted_record_is_gone(store):
    store.put_record_if_absent(record('k', 'first'))
    store.delete_record('k')
    assert store.get_record('k') is None
    assert store.put_record_if_absent(record('k', 'second')).outputs == {'answer': ['json', 'second']}


def test_a_version_is_stored_once(store):
    store.put_version('v1', {'code': None, 'files': {'a.txt': 'f'}, 'tools': {}, 'rev': '1'})
    store.put_version('v1', {'code': None, 'files': {}, 'tools': {}, 'rev': '2'})
    assert store.get_version('v1') == {'code': None, 'files': {'a.txt': 'f'}, 'tools': {}, 'rev': '1'}
    assert store.get_version('v2') is None


def open_and_put(path, files, barrier, i):
    barrier.wait()
    SqliteStore(path, files=files).put(f'blob {i}'.encode())


@pytest.mark.parametrize('wal_already', [False, True], ids=['new-file', 'empty-wal-db'])
def test_processes_opening_an_empty_db_at_once_create_the_schema_once(tmp_path, wal_already):
    n = 8
    path, files = tmp_path / 'stepcache.db', tmp_path / 'blobs'
    if wal_already:
        with closing(sqlite3.connect(path)) as c:
            c.execute('PRAGMA journal_mode = WAL')
    barrier = multiprocessing.Barrier(n)
    procs = [multiprocessing.Process(target=open_and_put, args=(path, files, barrier, i)) for i in range(n)]
    for p in procs:
        p.start()
    for p in procs:
        p.join(60)
    assert [p.exitcode for p in procs] == [0] * n
    store = SqliteStore(path, files=files)
    assert all(store.has(sha(f'blob {i}'.encode())) for i in range(n))


def test_opening_a_db_with_another_schema_version_is_refused(store):
    with closing(sqlite3.connect(store.path)) as c, c:
        c.execute("UPDATE meta SET value = 2 WHERE name = 'schema_version'")
    with pytest.raises(SchemaMismatch):
        SqliteStore(store.path, files=store.files)


def test_opening_a_db_that_is_not_a_stepcache_db_is_refused(tmp_path):
    with closing(sqlite3.connect(tmp_path / 'other.db')) as c, c:
        c.execute('CREATE TABLE notes (body TEXT)')
    with pytest.raises(SchemaMismatch):
        SqliteStore(tmp_path / 'other.db', files=tmp_path / 'blobs')


needs_fork = pytest.mark.skipif(not hasattr(os, 'fork'), reason='needs os.fork')


def test_a_claim_has_one_owner_until_the_owner_releases_it(store):
    other = SqliteStore(store.path, files=store.files)
    assert store.claim('k')
    assert not store.claim('k') and not other.claim('k')
    owner = store.claim_owner('k')
    other.release('k')
    assert not other.take_over('k', 'not the owner')
    assert other.claim_owner('k') == owner
    store.release('k')
    assert store.claim_owner('k') is None
    assert other.claim('k')
    assert other.claim_owner('k') != owner
    assert store.owner_alive(owner) and store.owner_alive(other.claim_owner('k'))


def claim_and_block(path, files, queue):
    store = SqliteStore(path, files=files)
    store.claim('k')
    queue.put(store.claim_owner('k'))
    time.sleep(60)


def test_a_killed_owner_is_dead_and_one_store_takes_its_claim_over(store):
    queue = multiprocessing.Queue()
    proc = multiprocessing.Process(target=claim_and_block, args=(store.path, store.files, queue))
    proc.start()
    try:
        owner = queue.get(timeout=30)
        assert store.owner_alive(owner)
        assert not store.claim('k')
        os.kill(proc.pid, signal.SIGKILL)
        proc.join()
        assert not store.owner_alive(owner)
        assert store.take_over('k', owner)
        assert not SqliteStore(store.path, files=store.files).take_over('k', owner)
        assert store.claim_owner('k') != owner
    finally:
        proc.kill()


def test_a_process_deletes_its_lock_file_when_it_exits(store):
    locks = store.path.with_name(f'{store.path.name}.locks')
    code = (
        'import sys; from stepcache import SqliteStore; '
        'store = SqliteStore(sys.argv[1], files=sys.argv[2]); store.claim("k"); store.release("k"); '
        'print(len(list(store.path.with_name(store.path.name + ".locks").iterdir())))'
    )
    out = subprocess.run([sys.executable, '-c', code, store.path, store.files], capture_output=True, text=True, check=True)
    assert out.stdout.strip() == '1'
    assert list(locks.iterdir()) == []


@needs_fork
def test_a_forked_child_claims_under_its_own_lock_file(store):
    assert store.claim('parent')
    pid = os.fork()
    if pid == 0:
        code = 1
        try:
            store.claim('child')
            code = 0
        finally:
            os._exit(code)
    _, status = os.waitpid(pid, 0)
    assert os.waitstatus_to_exitcode(status) == 0
    assert store.claim_owner('child') != store.claim_owner('parent')
    assert not store.owner_alive(store.claim_owner('child'))
    assert store.owner_alive(store.claim_owner('parent'))


def claim_fork_and_block(path, files, queue):
    store = SqliteStore(path, files=files)
    store.claim('k')
    child = os.fork()
    if child == 0:
        time.sleep(60)
        os._exit(0)
    queue.put((store.claim_owner('k'), child))
    time.sleep(60)


@needs_fork
def test_a_forked_child_that_never_uses_the_store_does_not_keep_a_dead_parent_alive(store):
    queue = multiprocessing.Queue()
    proc = multiprocessing.Process(target=claim_fork_and_block, args=(store.path, store.files, queue))
    proc.start()
    child = None
    try:
        owner, child = queue.get(timeout=30)
        os.kill(proc.pid, signal.SIGKILL)
        proc.join()
        assert not store.owner_alive(owner)
    finally:
        proc.kill()
        if child is not None:
            os.kill(child, signal.SIGKILL)
