
import pytest

TESTSET = [
    (
        {'name': {'$length': 5}},  # str is Sized
        lambda x: len(x['name']) == 5
    ), (
        {'bonus': {'$length': 2}},
        lambda x: len(x['bonus']) == 2
    ), (
        {'bonus': {'$length': {'$gt': 2}}},
        lambda x: len(x['bonus']) > 2
    ), (
        {'bonus': {'$length': {'$and': [{'$gt': 2}, {'$lt': 4}]}}},
        lambda x: (len(x['bonus']) > 2) and (len(x['bonus']) < 4)
    ),
    # syntactically okay but nonsence
    (
        {'bonus': {'$length': 'a'}},
        lambda x: False
    ), (
        {'bonus': {'$length': {'$search': 'a'}}},
        lambda x: False
    ), (
        {'bonus': {'$length': {'$and': [{'$search': 'a'}]}}},
        lambda x: False
    ),
    # reject non-sized data
    (
        {'age': {'$length': 2}},
        lambda x: False
    )
]

@pytest.mark.parametrize('spec', TESTSET)
def test_query(run_test_by_selector, spec):
    run_test_by_selector(spec)


ERRORSET = [
    {'$length': 5},
    {'$and': [{'$length': 5}]},
]

@pytest.mark.parametrize('spec', ERRORSET)
def test_error(run_test_error, spec):
    run_test_error(spec)
