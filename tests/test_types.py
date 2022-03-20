import numbers
import pytest

TESTSET = [
    (
        {'blob': {'$types': ['string']}},
        lambda x: isinstance(x['blob'], str)
    ), (
        {'blob': {'$types': ['number']}},
        lambda x: isinstance(x['blob'], numbers.Number)
    ), (
        {'blob': {'$types': ['boolean']}},
        lambda x: isinstance(x['blob'], bool)
    ), (
        {'blob': {'$types': ['array']}},
        lambda x: isinstance(x['blob'], list)
    ), (
        {'blob': {'$types': ['object']}},
        lambda x: isinstance(x['blob'], dict)
    ), (
        {'blob': {'$types': ['object', 'array']}},
        lambda x: isinstance(x['blob'], (dict, list))
    ), (
        {'status': {'$types': ['string']}},
        lambda x: False
    ), (
        {'status': {'$types': ['boolean']}},
        lambda x: False
    ), (
        {'name': {'$types': ['number']}},
        lambda x: False
    ), (
        {'name': {'$types': ['array']}},
        lambda x: False
    ), (
        {'name': {'$types': ['object']}},
        lambda x: False
    ), (
        {'name': {'$types': []}},
        lambda x: False
    )
]

@pytest.mark.parametrize('spec', TESTSET)
def test_by_selector(run_test_by_selector, spec):
    run_test_by_selector(spec)


ERRORSET = [
    {'name': {'$types': ['asdfw']}}
]

@pytest.mark.parametrize('spec', ERRORSET)
def test_error(run_test_error, spec):
    run_test_error(spec)
