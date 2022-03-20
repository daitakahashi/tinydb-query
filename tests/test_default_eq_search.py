
import pytest
import tinydb

# pylint: disable = singleton-comparison
TESTSET = [
    (
        {'name': 'hanako'},
        tinydb.Query().name == 'hanako',
        True
    ), (
        {'name': 'hana'},
        tinydb.Query().name == 'hana',
        False
    ), (
        {'name': {'$re': 'hana'}},
        tinydb.Query().name.search('hana'),
        True
    ), (
        {'age': 14},
        tinydb.Query().age == 14,
        True
    ), (
        {'status.gameover': True},
        tinydb.Query().status.gameover == True,
        True
    ), (
        {'name': 12},
        tinydb.Query().name == 12,
        False
    ), (
        {'rgearga': 12},
        tinydb.Query().rgearga == 12,
        False
    ), (
        {'age': {'a': 12}},
        tinydb.Query().age.a == 12,
        False
    ), (
        {'name': {'$or': ['alice', {'$gt': 'ichiro'}]}},
        (tinydb.Query().name == 'alice') | (tinydb.Query().name > 'ichiro'),
        True
    )
]

@pytest.mark.parametrize('spec', TESTSET)
def test_query(run_test_query, spec):
    run_test_query(spec)


ERRORSET = [
    # no toplevel
    12,
    'name',
    True,
    {'$and': [12, {'name': {'$eq': 'alice'}}]},
    # what the default matching for an array means is non-trivial,
    # and treated as an error
    {'bonus': ['book', 'key']},
    {'bonus': []},
    {'age': [12, 13]},
    {'age': []}
]

@pytest.mark.parametrize('spec', ERRORSET)
def test_error(run_test_error, spec):
    run_test_error(spec)
