import importlib.util
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from stepcache import SqliteStore


@pytest.fixture
def load_module(tmp_path):
    def load(name, source):
        path = tmp_path / f'{name}.py'
        path.write_text(source)
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    return load


@pytest.fixture
def store(tmp_path):
    return SqliteStore(tmp_path / 'cache' / 'stepcache.db', files=tmp_path / 'cache' / 'blobs')


@pytest.fixture
def clock(monkeypatch):
    clock = SimpleNamespace(now=datetime(2026, 1, 1, 9, tzinfo=timezone.utc))
    monkeypatch.setattr('stepcache.store.now', lambda: clock.now)
    return clock
