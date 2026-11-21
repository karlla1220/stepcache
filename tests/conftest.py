import importlib.util

import pytest


@pytest.fixture
def load_module(tmp_path):
    """Write `source` to a file and import it, so its functions have a source file."""

    def load(name, source):
        path = tmp_path / f'{name}.py'
        path.write_text(source)
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    return load
