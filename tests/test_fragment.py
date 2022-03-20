
import pytest
import tinydb

TESTSET = [
    (
        {'$fragment': {'age': 14, 'name': 'alice'}},
        tinydb.Query().fragment({'age': 14, 'name': 'alice'}),
        True
    ), (
        {'status': {'$fragment': {'gameover': False, 'cleared': False}}},
        tinydb.Query().status.fragment({'gameover': False, 'cleared': False}),
        True
    )
]

@pytest.mark.parametrize('spec', TESTSET)
def test_query(run_test_query, spec):
    run_test_query(spec)


ERRORSET = [
    {'age': {'$fragment': 12}},
    {'bonus': {'$fragment': ['key', 'book', 'orb']}}
]

@pytest.mark.parametrize('spec', ERRORSET)
def test_error(run_test_error, spec):
    run_test_error(spec)
