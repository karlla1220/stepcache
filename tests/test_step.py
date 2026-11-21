import functools
import hashlib
import shutil
import sys
from pathlib import Path

import pytest

from stepcache import Blob, Step, Version


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def run(inputs):
    return {}


def defined_without_a_file():
    namespace = {}
    exec('def run(inputs):\n    return {}\n', namespace)
    return namespace['run']


def test_blob_is_addressed_by_its_sha256():
    blob = Blob(b'png bytes')
    assert blob.sha256 == sha(b'png bytes')
    assert blob.read() == b'png bytes'
    assert blob == Blob(b'png bytes')
    assert blob != Blob(b'other bytes')
    assert len({blob, Blob(b'png bytes')}) == 1


@pytest.mark.parametrize('data', ['text', 3])
def test_blob_takes_only_bytes(data):
    with pytest.raises(TypeError):
        Blob(data)


def test_step_puts_the_hash_of_the_run_module_in_its_version():
    step = Step('s', Version.of(), run)
    assert step.version.components['code'] == sha(Path(__file__).read_bytes())


def test_code_false_leaves_the_code_out():
    assert Step('s', Version.of(code=False), run).version.components['code'] is None


def test_a_run_batch_from_another_module_is_hashed_too(load_module):
    a = load_module('steps_a', 'def run(inputs):\n    return {}\n\ndef run_batch(xs):\n    return []\n')
    b = load_module('steps_b', 'def run_batch(xs):\n    return []\n')
    run_only = Step('s', Version.of(), a.run).version.components['code']
    same_module = Step('s', Version.of(), a.run, run_batch=a.run_batch).version.components['code']
    other_module = Step('s', Version.of(), a.run, run_batch=b.run_batch).version.components['code']
    assert same_module == run_only
    assert other_module != run_only


@pytest.mark.parametrize('make_run', [lambda: len, defined_without_a_file], ids=['builtin', 'exec'])
def test_a_run_without_a_source_file_is_refused(make_run):
    with pytest.raises(ValueError):
        Step('s', Version.of(), make_run())
    assert Step('s', Version.of(code=False), make_run()).version.components['code'] is None


@pytest.mark.parametrize(
    'make_run',
    [
        lambda m: functools.partial(m.run, scale=2),
        lambda m: m.Run(),
    ],
    ids=['partial', 'callable-instance'],
)
def test_a_partial_or_callable_instance_is_hashed_by_its_defining_module(load_module, make_run, monkeypatch):
    module = load_module('steps', (
        'def run(inputs, scale):\n    return {}\n\n'
        'class Run:\n    def __call__(self, inputs):\n        return {}\n'
    ))
    # inspect finds a class's file through sys.modules, where an import would have put it.
    monkeypatch.setitem(sys.modules, 'steps', module)
    step = Step('s', Version.of(), make_run(module))
    assert step.version.components['code'] == sha(Path(module.__file__).read_bytes())


def make_tree(root: Path):
    (root / 'cli' / 'sub').mkdir(parents=True)
    (root / 'cli' / 'main.rs').write_bytes(b'fn main() {}')
    (root / 'cli' / 'sub' / 'lib.rs').write_bytes(b'pub fn f() {}')
    (root / 'cli' / '.hidden').write_bytes(b'editor state')
    (root / 'Cargo.lock').write_bytes(b'lock')
    (root / 'README').write_bytes(b'not declared')


def test_version_files_hashes_the_matching_files_by_relative_path(tmp_path):
    make_tree(tmp_path / 'src')
    files = Version.files(tmp_path / 'src', ['cli/**', 'Cargo.lock'])
    assert files == {
        'Cargo.lock': sha(b'lock'),
        'cli/main.rs': sha(b'fn main() {}'),
        'cli/sub/lib.rs': sha(b'pub fn f() {}'),
    }
    assert list(files) == sorted(files)


def test_version_files_does_not_depend_on_where_the_tree_is(tmp_path):
    make_tree(tmp_path / 'here')
    shutil.copytree(tmp_path / 'here', tmp_path / 'elsewhere' / 'there')
    patterns = ['cli/**/*.rs', 'Cargo.lock']
    assert Version.files(tmp_path / 'here', patterns) == Version.files(tmp_path / 'elsewhere' / 'there', patterns)


def test_version_files_refuses_a_pattern_that_matches_nothing(tmp_path):
    make_tree(tmp_path / 'src')
    with pytest.raises(ValueError):
        Version.files(tmp_path / 'src', ['cli/**', 'Cargo.toml'])
