
import pytest
import tinydb

# pylint: disable = singleton-comparison
TESTSET = [(
        {'$or': []},
        ~tinydb.Query().noop(),
        False
    ), (
        {'$or': [{'status.gameover': False}]},
        tinydb.Query().status.gameover == False,
        True
    ), (
        {'$or': [{'status.gameover': {'$exists': True}}, {'age': 12}]},
        (tinydb.Query().status.gameover.exists())
        | (tinydb.Query().age == 12),
        True
    ), (
        {'$or': [{'status.gameover': False}, {'age': 14},
                 {'name': 'bob'}]},
        (tinydb.Query().status.gameover == False)
        | (tinydb.Query().age == 14)
        | (tinydb.Query().name == 'bob'),
        True
    ), (
        {'$or': [{'$or': [{'name': 'bob'}]}]},
        tinydb.Query().name == 'bob',
        True
    ), (
        {'status': {'$or': [{'gameover': True}, {'lang': 'jp'}]}},
        (tinydb.Query().status.gameover == True)
        | (tinydb.Query().status.lang == 'jp'),
        True
    ), (
        {'status': {'$or': [{'$exists': True}, {'lang': 'jp'}]}},
        tinydb.Query().status.exists() | (tinydb.Query().status.lang == 'jp'),
        True
    ), (
        {'age': {'$or': [12, 14]}},
        (tinydb.Query().age == 12) | (tinydb.Query().age == 14),
        True
    )
]

@pytest.mark.parametrize('spec', TESTSET)
def test_query(run_test_query, spec):
    run_test_query(spec)


ERRORSET = [
    {'$or': [], 'something': ''},
    {'$or': True},
    {'name': {'$or': True}},
    {'$or': [{'$or': [{'$exists': True}]}]},
]

@pytest.mark.parametrize('spec', ERRORSET)
def test_error(run_test_error, spec):
    run_test_error(spec)
