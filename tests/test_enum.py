
import pytest
import tinydb

TESTSET = [
    (
        {'age': {'$enum': [12, 13, 14]}},
        tinydb.Query().age.one_of([12, 13, 14]),
        True
    ), (
        {'age': {'$enum': []}},
        tinydb.Query().age.one_of([]),
        False
    ), (
        {'status.lang': {'$enum': ['jp']}},
        tinydb.Query().status.lang.one_of(['jp']),
        True
    )
]

@pytest.mark.parametrize('spec', TESTSET)
def test_query(run_test_query, spec):
    run_test_query(spec)


ERRORSET = [
    {'$enum': [1, 2, 3]},
    {'$and': [{'$enum': [1, 2, 3]}]},
    {'$or': [{'$enum': [1, 2, 3]}]},
    {'$not': [{'$enum': [1, 2, 3]}]},
    {'status.lang': {'$enum': 12}},
    {'status.lang': {'$enum': {'$gt': 12}}}
]

@pytest.mark.parametrize('spec', ERRORSET)
def test_error(run_test_error, spec):
    run_test_error(spec)
