
import pytest
import tinydb

TESTSET = [
    (
        {'status.current-stage': {'$exists': True}},
        tinydb.Query().status['current-stage'].exists(),
        True
    ), (
        {'status.current-stage': {'$exists': False}},
        ~tinydb.Query().status['current-stage'].exists(),
        True
    )
]

@pytest.mark.parametrize('spec', TESTSET)
def test_query(run_test_query, spec):
    run_test_query(spec)


ERRORSET = [
    # needs path
    {'$exists': True},
    {'$and': [{'$exists': {'name': True}}]},
    {'$or': [{'$exists': {'name': True}}]},
    {'$not': {'$exists': {'name': True}}},
    {'name': {'$exists': True, 'other-parameter': False}},
]

@pytest.mark.parametrize('spec', ERRORSET)
def test_error(run_test_error, spec):
    run_test_error(spec)
