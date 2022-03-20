
import pytest
import tinydb

# pylint: disable = singleton-comparison
TESTSET = [(
        {'$and': []},
        tinydb.Query().noop(),
        True
    ), (
        {'$and': [{'status.gameover': False}]},
        tinydb.Query().status.gameover == False,
        True
    ), (
        {'$and': [{'status.gameover': {'$exists': True}}, {'age': 12}]},
        (tinydb.Query().status.gameover.exists())
        & (tinydb.Query().age == 12),
        True
    ), (
        {'$and': [{'status.gameover': False}, {'age': 12},
                  {'name': 'bob'}]},
        (tinydb.Query().status.gameover == False)
        & (tinydb.Query().age == 12)
        & (tinydb.Query().name == 'bob'),
        True
    ), (
        {'$and': [{'$and': [{'name': 'bob'}]}]},
        tinydb.Query().name == 'bob',
        True
    ), (
        {'status': {'$and': [{'gameover': True}, {'lang': 'jp'}]}},
        (tinydb.Query().status.gameover == True)
        & (tinydb.Query().status.lang == 'jp'),
        True
    ), (
        {'status': {'$and': [{'$exists': True}, {'lang': 'jp'}]}},
        tinydb.Query().status.exists() & (tinydb.Query().status.lang == 'jp'),
        True
    ), (
        {'age': {'$and': [12]}},
        tinydb.Query().age == 12,
        True
    )
]

@pytest.mark.parametrize('spec', TESTSET)
def test_query(run_test_query, spec):
    run_test_query(spec)


ERRORSET = [
    {'$and': [], 'something': ''},
    {'$and': True},
    {'name': {'$and': True}},
    {'$and': [{'$and': [{'$exists': True}]}]},
]

@pytest.mark.parametrize('spec', ERRORSET)
def test_error(run_test_error, spec):
    run_test_error(spec)
