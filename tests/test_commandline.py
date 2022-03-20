
import pytest

from tinydb_ql.__main__ import _main


ARGS = [
    ['{"age": 12}'],
    ['{"age": 12}', '--json'],
    ['{"age": 12}', '--sample', '3']
]

@pytest.mark.parametrize('arg', ARGS)
def test_commandline_run(db_path, arg):
    _main([
        'main', str(db_path), *arg
    ])
