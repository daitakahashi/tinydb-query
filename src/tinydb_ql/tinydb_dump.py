#!/usr/bin/env python3

from argparse import ArgumentParser
from pathlib import Path
import json
import sys

import tinydb
from tinydb.storages import JSONStorage
from tinydb.middlewares import CachingMiddleware

def parse_args(argv):
    parser = ArgumentParser()
    parser.add_argument(
        'output', type=Path,
        help='output tinydb DB path'
    )
    args = parser.parse_args(argv)
    return args


def insert_from_array(db, array_input):
    db.insert_multiple(array_input)


def insert_from_object(db, object_input):
    tables = db.storage.read()
    if tables is None:
        tables = {}
    table_name = db.default_table_name
    raw_table = {
        str(key): value
        for key, value in tables.get(table_name, {})
    }
    documents = {
        str(int(key)): value
        for key, value in object_input.items()
    }
    raw_table.update(documents)
    tables[table_name] = raw_table
    db.storage.write(tables)
    db.table(table_name).clear_cache()


def main():
    try:
        _main(sys.argv)
    except RuntimeError as exc:
        print(exc, file=sys.stderr)
        sys.exit(-1)


def _main(argv):
    args = parse_args(argv[1:])
    input_json = json.load(sys.stdin)
    with tinydb.TinyDB(
            args.output, storage=CachingMiddleware(JSONStorage)
    ) as db:
        if isinstance(input_json, list):
            insert_from_array(db, input_json)
        elif isinstance(input_json, dict):
            insert_from_object(db, input_json)
        else:
            raise RuntimeError('input must be a list or a dictionary')


if __name__ == '__main__':
    main()
