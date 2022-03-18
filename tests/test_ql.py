
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
             'BVW': 1051},
            {'title': 'Sonata in C minor', 'composer': 'Franz Schubert',
             'key': 'C minor',
             'D': 958},
            {'title': 'Sonata in A mojor', 'composer': 'Franz Schubert',
             'key': 'A mojor',
             'D': 959},
            {'title': 'Sonata in B-flat mojor', 'composer': 'Franz Schubert',
             'key': 'B-flat mojor',
             'D': 960}
        ])
        yield db


@pytest.mark.parametrize(('ql', 'tdb_query', 'exists'), [
    (
        {'D': {'$exists': True}},
        tinydb.Query().D.exists(),
        True
    ), (
        {'D': {'$exists': False}},
        ~tinydb.Query().D.exists(),
        True
    ), (
        {'title': {'$matches': 'Gold'}},
        tinydb.Query().title.matches('Gold'),
        True
    ), (
        {'title': {'$matches': 'Con.*$'}},
        tinydb.Query().title.matches('Con.*$'),
        False
    ), (
        {'title': {'$matches': '^Gold.*ns$'}},
        tinydb.Query().title.matches('^Gold.*ns$'),
        True
    ), (
        {'title': {'$matches': {'$re': '^Gold.*ns$'}}},
        tinydb.Query().title.matches('^Gold.*ns$'),
        True
    ), (
        {'key': {'$search': 'flat'}},
        tinydb.Query().key.search('flat'),
        True
    ), (
        {'title': {'$search': {'$re': 'C.*o'}}},
        tinydb.Query().title.search('C.*o'),
        True
    ), (
        {'title': {'$re': 'C.*o'}},
        tinydb.Query().title.search('C.*o'),
        True
    ), (
        {'$fragment': {'no': 6, 'composer': 'J. S. Bach'}},
        tinydb.Query().fragment({'no': 6, 'composer': 'J. S. Bach'}),
        True
    ), (
        {'$and': [{'composer': 'J. S. Bach'}, {'no': {'$gt': 2}}]},
        (tinydb.Query().composer == 'J. S. Bach') & (tinydb.Query().no > 2),
        True
    ), (
        {'composer': 'J. S. Bach', 'no': {'$gt': 2}},
        (tinydb.Query().composer == 'J. S. Bach') & (tinydb.Query().no > 2),
        True
    ), (
        {'title': {'$and': [{'$search': 'Sonata'}, {'$search': 'A'}]}},
        tinydb.Query().title.search('Sonata') & tinydb.Query().title.search('A'),
        True
    ), (
        {'title': {'$and': [{'$re': 'Sonata'}, {'$re': 'A'}]}},
        tinydb.Query().title.search('Sonata') & tinydb.Query().title.search('A'),
        True
    ), (
        {'$or': [{'composer': 'Franz Schubert'}, {'no': {'$gt': 2}}]},
        (tinydb.Query().composer == 'Franz Schubert') | (tinydb.Query().no > 2),
        True
    ), (
        {'title': {'$or': [{'$search': 'Sonata'}, {'$search': 'G'}]}},
        tinydb.Query().title.search('Sonata') | tinydb.Query().title.search('G'),
        True
    ), (
        {'key': {'$or': ['F major', 'minor']}},
        (tinydb.Query().key == 'F major') | (tinydb.Query().key == 'minor'),
        True
    ), (
        {'key': {'$or': ['F major', {'$re': 'minor'}]}},
        (tinydb.Query().key == 'F major') | (tinydb.Query().key.search('minor')),
        True
    )
])
def test_query(db_instance, ql, tdb_query, exists):
    result = db_instance.search(Query(ql))
    assert bool(result) is exists
    assert result == db_instance.search(tdb_query)


@pytest.mark.parametrize(('ql', 'exception'), [
    (
        {'$and': [{'$search': 'J. S. Bach'}]},
        ValueError
     ), (
        {'$or': [{'$search': 'J. S. Bach'}]},
        ValueError
    )
])
def test_query_error(ql, exception):
    with pytest.raises(exception):
        Query(ql)
