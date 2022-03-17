
import pytest
import tinydb
from tinydb_ql import Query


@pytest.fixture(name='db_instance')
def _db_instance(tmp_path):
    with tinydb.TinyDB(tmp_path / 'db.json') as db:
        db.insert_multiple([
            {'title': 'Goldberg Variations', 'composer': 'J. S. Bach',
             'BVW': 988},
            {'title': 'Brandenburg Concertos', 'composer': 'J. S. Bach',
             'no': 1, 'key': 'F major',
             'BVW': 1046},
            {'title': 'Brandenburg Concertos', 'composer': 'J. S. Bach',
             'no': 2, 'key': 'F major',
             'BVW': 1047},
            {'title': 'Brandenburg Concertos', 'composer': 'J. S. Bach',
             'no': 3, 'key': 'G major',
             'BVW': 1048},
            {'title': 'Brandenburg Concertos', 'composer': 'J. S. Bach',
             'no': 4, 'key': 'G major',
             'BVW': 1049},
            {'title': 'Brandenburg Concertos', 'composer': 'J. S. Bach',
             'no': 5, 'key': 'D major',
             'BVW': 1050},
            {'title': 'Brandenburg Concertos', 'composer': 'J. S. Bach',
             'no': 6, 'key': 'B-flat major',
             'BVW': 1051}
        ])
        yield db


def test_query_exists(db_instance):
    result = db_instance.search(
        Query({'no': {'$exists': True}}).as_tinydb_query()
    )
    assert len(result) == 6

    result = db_instance.search(
        Query({'no': {'$exists': False}}).as_tinydb_query()
    )
    assert len(result) == 1


def test_query_matches(db_instance):
    result = db_instance.search(
        Query({'title': {'$matches': 'Gold'}}).as_tinydb_query()
    )
    assert len(result) == 1

    result = db_instance.search(
        Query({'title': {'$matches': {'$regex': '^Gold.*ns$'}}}).as_tinydb_query()
    )
    assert len(result) == 1


def test_query_search(db_instance):
    result = db_instance.search(
        Query({'key': {'$search': 'flat'}}).as_tinydb_query()
    )
    assert len(result) == 1
