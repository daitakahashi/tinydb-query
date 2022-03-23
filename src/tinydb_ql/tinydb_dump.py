#!/usr/bin/env python3

import json
import sys
from argparse import ArgumentParser
from distutils.version import LooseVersion
from pathlib import Path

import tinydb
from tinydb.middlewares import CachingMiddleware
from tinydb.storages import JSONStorage


def parse_args(argv):
    parser = ArgumentParser()
    parser.add_argument(
        'output', type=Path, help='output tinydb DB path'
    )
    args = parser.parse_args(argv)
    return args


def insert_from_array(db, array_input):
    db.insert_multiple(array_input)


def insert_from_object(db, object_input):
    table_name = db.default_table_name
    id_class = db.table(table_name).document_id_class
    content = db.storage.read()
    if content is None:
        content = {}
    table = content.get(table_name, {})
    existing = {
        str(id_class(key)): doc
        for key, doc in table.items()
    }
    documents = {
        str(id_class(key)): value
        for key, value in object_input.items()
    }
    existing.update(documents)
    content[table_name] = existing
    db.storage.write(content)
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
