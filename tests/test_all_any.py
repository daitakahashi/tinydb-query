import re

import pytest
import tinydb

TESTSET = [
    (
        {'bonus': {'$all': ['book', 'key']}},
        tinydb.Query().bonus.all(['book', 'key']),
        True
    ), (
        {'age': {'$all': [12, 13, 14]}},
        tinydb.Query().age.all([12, 13, 14]),
        False
    ), (
        {'status.by-stage': {'$all': [{'stage': 'stage1', 'score': 100},
                                      {'stage': 'stage2', 'score': 80}]}},
        tinydb.Query().status['by-stage'].all(
            [{'stage': 'stage1', 'score': 100},
             {'stage': 'stage2', 'score': 80}]
        ),
        True
    ), (
        {'status.by-stage': {'$all': {'stage': {'$re': '[1-3]'}}}},
        tinydb.Query().status['by-stage'].all(
            tinydb.Query().stage.search('[1-3]')
        ),
        True
    ), (
        {'bonus': {'$any': ['book', 'key']}},
        tinydb.Query().bonus.any(['book', 'key']),
        True
    ), (
        {'age': {'$any': [12, 13, 14]}},
        tinydb.Query().age.any([12, 13, 14]),
        False
    ), (
        {'status.by-stage': {'$any': {'score': {'$lt': 70}}}},
        tinydb.Query().status['by-stage'].any(
            tinydb.Query().score < 70
        ),
        True
    ), (
        {'status.by-stage': {'$any': [{'stage': 'stage3', 'score': 90}]}},
        tinydb.Query().status['by-stage'].any([{'stage': 'stage3', 'score': 90}]),
        True
    )
]

@pytest.mark.parametrize('spec', TESTSET)
def test_query(run_test_query, spec):
    run_test_query(spec)


TESTSET_BY_SELECTOR = [
    (
        {'bonus': {'$all': {'$re': '[ko]'}}},
        lambda x: all(
            re.search('[ko]', elem) for elem in x['bonus']
        )
    ), (
        {'bonus': {'$any': {'$re': 'candela'}}},
        lambda x: any(
            re.search('candela', elem) for elem in x['bonus']
        )
    )
]

@pytest.mark.parametrize('spec', TESTSET_BY_SELECTOR)
def test_by_selector(run_test_by_selector, spec):
    run_test_by_selector(spec)


ERRORSET = [
    # no toplevel
    {'$all': []},
    {'$and': [{'$all': []}]},
    {'$any': []},
    {'$and': [{'$any': []}]},
    # no extra properties
    {'bonus': {'$all': [], 'other': 1}},
    {'bonus': {'$any': [], 'other': 1}}
]

@pytest.mark.parametrize('spec', ERRORSET)
def test_error(run_test_error, spec):
    run_test_error(spec)
