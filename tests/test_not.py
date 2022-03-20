
import pytest
import tinydb

TESTSET = [
    (
        {'$not': {'name': 'bob'}},
        ~(tinydb.Query().name == 'bob'),
        True
    ),(
        {'$not': {'$not': {'name': 'bob'}}},
        tinydb.Query().name == 'bob',
        True
    ), (
        {'$not': {}},
        ~(tinydb.Query().noop()),
        False,
    ), (
        {'status.current-stage': {'$not': 3}},
        ~(tinydb.Query().status['current-stage'] == 3),
        True
    ), (
        {'status.current-stage': {'$not': {'$lt': 4}}},
        ~(tinydb.Query().status['current-stage'] < 4),
        True
    ), (
        {'status.current-stage': {'$not': {'$not': {'$lt': 4}}}},
        ~~(tinydb.Query().status['current-stage'] < 4),
        True
    )
]

@pytest.mark.parametrize('spec', TESTSET)
def test_query(run_test_query, spec):
    run_test_query(spec)


ERRORSET = [
    {'$not': True},
    {'$not': []},
    {'$not': {'age': 14}, 'other': 12},
    {'age': {'$not': 14, 'other': 12}}
]

@pytest.mark.parametrize('spec', ERRORSET)
def test_error(run_test_error, spec):
    run_test_error(spec)
