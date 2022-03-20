
import pytest
import tinydb

TESTSET = [
    (
        {'age': {'$eq': 13}},
        tinydb.Query().age == 13,
        True
    ), (
        {'age': {'$ne': 13}},
        tinydb.Query().age != 13,
        True
    ), (
        {'age': {'$gt': 13}},
        tinydb.Query().age > 13,
        True
    ), (
        {'age': {'$ge': 13}},
        tinydb.Query().age >= 13,
        True
    ), (
        {'age': {'$lt': 13}},
        tinydb.Query().age < 13,
        True
    ), (
        {'age': {'$le': 13}},
        tinydb.Query().age <= 13,
        True
    ), (
        {'age': {'$eq': '13'}},
        tinydb.Query().age == '13',
        False
    ), (
        {'status': {'$eq': '13'}},
        tinydb.Query().status == '13',
        False
    ),
    # == and != are generic
    (
        {'age': {'$ne': '13'}},
        tinydb.Query().age != '13',
        True
    ), (
        {'status': {'$ne': '13'}},
        tinydb.Query().status != '13',
        True
    )
]

@pytest.mark.parametrize('spec', TESTSET)
def test_query(run_test_query, spec):
    run_test_query(spec)


ERRORSET = [
    {'$eq': 12},
    {'$ne': 12},
    {'$ge': 12},
    {'$gt': 12},
    {'$le': 12},
    {'$lt': 12},
    {'$and': [{'$eq': 12}]},
    {'$and': [{'$ne': 12}]},
    {'$and': [{'$ge': 12}]},
    {'$and': [{'$gt': 12}]},
    {'$and': [{'$le': 12}]},
    {'$and': [{'$lt': 12}]},
    {'$or': [{'$eq': 12}]},
    {'$or': [{'$ne': 12}]},
    {'$or': [{'$ge': 12}]},
    {'$or': [{'$gt': 12}]},
    {'$or': [{'$le': 12}]},
    {'$or': [{'$lt': 12}]},
    {'$not': {'$eq': 12}},
    {'$not': {'$ne': 12}},
    {'$not': {'$ge': 12}},
    {'$not': {'$gt': 12}},
    {'$not': {'$le': 12}},
    {'$not': {'$lt': 12}},
]

@pytest.mark.parametrize('spec', ERRORSET)
def test_error(run_test_error, spec):
    run_test_error(spec)
