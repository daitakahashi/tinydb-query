
import pytest
import tinydb

# pylint: disable = singleton-comparison
TESTSET = [
    (
        # simple selector
        {'name': 'bob'},
        tinydb.Query().name == 'bob',
        True
    ), (
        # compounded selector
        {'status.gameover': False},
        tinydb.Query().status.gameover == False,
        True
    ), (
        # multiple selectors (toplevel)
        {'name': 'bob', 'status.gameover': False},
        (tinydb.Query().name == 'bob')
        & (tinydb.Query().status.gameover == False),
        True
    ), (
        # multiple selectors
        {'status': {'gameover': False, 'cleared': True}},
        (tinydb.Query().status.gameover == False)
        & (tinydb.Query().status.cleared == True),
        True
    ), (
        # absent field
        {'sdrgreaa': 12},
        tinydb.Query().sdrgreaa == 12,
        False
    ), (
        {'sdrgr.eaa': 12},
        tinydb.Query().sdrgr.eaa == 12,
        False
    ), (
        # field name with hyphen
        {'status.current-stage': 4},
        tinydb.Query().status['current-stage'] == 4,
        True
    )
]

@pytest.mark.parametrize('spec', TESTSET)
def test_query(run_test_query, spec):
    run_test_query(spec)


ERRORSET = [
    # '$' is not allowed
    {'status': {'current-stage': 2, '$exists': True}},
    {'status': {'current-stage': 2, '$exfreaists': 1}},
    {'status.$exists': True},
    {'status.$exists.er': True},
    {'status.ar$exists.er': True},

    # cannot start from '.'
    {'.arexists.er': True},

    # empty field-name is not allowed
    {'status..arexists.er': True}
]

@pytest.mark.parametrize('spec', ERRORSET)
def test_error(run_test_error, spec):
    run_test_error(spec)
