#!/usr/bin/env python3

import contextlib
import json
import pprint
import random
import sys
from argparse import ArgumentParser
from pathlib import Path

import tinydb
from tinydb.middlewares import CachingMiddleware
from tinydb.storages import JSONStorage

from .tinydb_ql import QLSyntaxError, Query, Schema


@contextlib.contextmanager
def load_data(dbpath, table_name):
    if not isinstance(dbpath, Path):
        raise RuntimeError('no input file')
    if not dbpath.exists():
        raise FileNotFoundError('input file does not exist')
    if not dbpath.is_file():
        raise IsADirectoryError('input path is not a file')
    with tinydb.TinyDB(
            dbpath, access_mode='r', storage=CachingMiddleware(JSONStorage)
    ) as db:
        if table_name is None:
            yield db, 'default table'
        else:
            available_tables = db.tables()
            if table_name in available_tables:
                yield db.table(table_name), f'table {table_name}'
            else:
                raise RuntimeError(f'available tables: {available_tables}')


def parse_args(argv):
    def positive_or_none(x):
        if x <= 0:
            return None
        return int(x)

    parser = ArgumentParser(description='Query documents in a tinydb db.')
    parser.add_argument(
        'db_path', nargs='?', default=None, type=Path, help='input db'
    )
    parser.add_argument(
        'query', nargs='?', default='{}', help='DB query (JSON formatted)'
    )
    parser.add_argument(
        '--schema', action='store_true',
        help='show a query-language jsonschema'
    )
    parser.add_argument(
        '--table', help='target table (default: the default table)'
    )
    parser.add_argument(
        '--max-depth', type=int,
        help='maximum depth to show (a value <= 0 means unlimited)'
    )
    parser.add_argument(
        '--with-index', action='store_true',
        help='display as an indexed dictionary'
    )
    parser.add_argument(
        '--sample', type=int,
        metavar='N',
        help='sample N documents randomly'
    )
    parser.add_argument(
        '--json', action='store_true',
        help='output as a JSON text'
    )
    args = parser.parse_args(argv)
    if args.max_depth is not None:
        args.max_depth_specified = True
        args.max_depth = positive_or_none(args.max_depth)
    else:
        args.max_depth_specified = False
        args.max_depth = 1
    return args


def main():
    try:
        _main(sys.argv)
        return 0
    except QLSyntaxError as exc:
        print(f'Query-syntax error: {str(exc)}', file=sys.stderr)
    except (FileNotFoundError, IsADirectoryError, RuntimeError) as exc:
        print(str(exc), file=sys.stderr)
    sys.exit(-1)


def _main(argv):
    args = parse_args(argv[1:])
    if args.schema:
        if args.json:
            json.dump(Schema(), sys.stdout, indent=4)
            print()
        else:
            pprint.pp(Schema())
        return
    with load_data(args.db_path, args.table) as (db, table_msg):
        try:
            query = Query(json.loads(args.query))
        except json.decoder.JSONDecodeError as exc:
            raise QLSyntaxError(str(exc)) from exc
        result = db.search(query)
    result_count = len(result)
    if len(result) > 1 or args.max_depth_specified:
        pp_options = {
            'depth': args.max_depth + 1 if args.max_depth is not None else None
        }
    else:
        pp_options = {}
    plural = '' if result_count == 1 else 's'
    summary_txt = f'found {result_count} document{plural} on the {table_msg}'
    if args.sample is not None:
        sample_count = min(result_count, args.sample)
        result = sorted(random.sample(result, sample_count), key=lambda x: x.doc_id)
        summary_txt += f' ({sample_count} sampled).'
    else:
        summary_txt += '.'
    if args.with_index:
        result = {doc.doc_id: doc for doc in result}
    if args.json:
        if isinstance(result, dict):
            result = {str(key): value for key, value in result.items()}
        print(json.dumps(result))
    else:
        pprint.pp(result, **pp_options)
    print(summary_txt, file=sys.stderr)


if __name__ == '__main__':
    main()
