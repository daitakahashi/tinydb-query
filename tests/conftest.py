
import contextlib

import pytest
import tinydb

import tinydb_ql as QL

@contextlib.contextmanager
def _db_instance(*args, **kwargs):
    with tinydb.TinyDB(*args, **kwargs) as db:
        db.insert_multiple([
            {'name': 'bob', 'age': 12,
             'blob': 1,
             'status': {
                 'gameover': False, 'cleared': False,
                 'current-stage': 3,
                 'by-stage': [{'stage': 'stage1', 'score': 100},
                              {'stage': 'stage2', 'score': 70},
                              {'stage': 'stage3', 'score': 50}]
             },
             'bonus': ['key', 'book', 'orb']},
            {'name': 'alice', 'age': 14,
             'blob': True,
             'status': {
                 'gameover': False, 'cleared': True,
                 'by-stage': [{'stage': 'stage1', 'score': 100},
                              {'stage': 'stage2', 'score': 80},
                              {'stage': 'stage3', 'score': 90},
                              {'stage': 'stage4', 'score': 80},
                              {'stage': 'stage5', 'score': 100}]
             },
             'bonus': ['key', 'book', 'orb',  'candle']},
            {'name': 'taro', 'age': 13,
             'blob': '1',
             'status': {
                 'gameover': True, 'cleared': False,
                 'lang': 'jp',
                 'current-stage': 3,
                 'by-stage': [{'stage': 'stage1', 'score': 100},
                              {'stage': 'stage2', 'score': 80},
                              {'stage': 'stage3', 'score': 40}]
             },
             'bonus': ['key', 'book']},
            {'name': 'hanako', 'age': 15,
             'blob': [1, 2],
             'status': {
                 'gameover': False, 'cleared': False,
                 'lang': 'jp',
                 'current-stage': 4,
                 'by-stage': [{'stage': 'stage1', 'score': 80},
                              {'stage': 'stage2', 'score': 80},
                              {'stage': 'stage3', 'score': 100},
                              {'stage': 'stage4', 'score': 60}]
             },
             'bonus': ['book', 'candelabrum']},
            {'name': 'ichiro', 'age': 16,
             'blob': {'a': 2},
             'status': {
                 'gameover': False, 'cleared': False,
                 'lang': 'jp',
                 'current-stage': 2,
                 'by-stage': [{'stage': 'stage1', 'score': 80},
                              {'stage': 'stage2', 'score': 80}]
             },
             'bonus': ['book', 'candle']}
        ])
        yield db


@pytest.fixture(name='db_instance')
def onmemory_db():
    with _db_instance(storage=tinydb.storages.MemoryStorage) as db:
        yield db


@pytest.fixture(name='db_path')
def fs_db(tmp_path):
    db_path = tmp_path / 'db.json'
    with _db_instance(db_path):
        return db_path


@pytest.fixture(name='run_test_query')
def _run_test_query(db_instance):
    def runner(test_spec):
        ql, tinydb_query, should_exists = test_spec
        q = QL.Query(ql)
        result = db_instance.search(q)
        assert bool(result) is should_exists, q
        assert result == db_instance.search(tinydb_query), q
    yield runner


@pytest.fixture(name='run_test_by_selector')
def _run_test_by_selector(db_instance):
    def runner(test_spec):
        ql, selector = test_spec
        q = QL.Query(ql)
        result = db_instance.search(q)
        expected = list(filter(selector, db_instance.all()))
        assert result == expected, q
    yield runner


@pytest.fixture(name='run_test_error')
def _run_test_error():
    def runner(test_spec):
        ql = test_spec
        with pytest.raises(QL.QLSyntaxError):
            QL.Query(ql)
    yield runner
