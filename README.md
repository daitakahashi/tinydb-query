# tinydb-query
A commandline tool to query a TinyDB database

## Basic usage

```python
>>> import tinydb
>>> db = tinydb.TinyDB('db.json')
>>> db.insert_multiple([
...    {'name': 'John', 'age': 22},
...    {'name': 'John', 'age': 37},
...    {'name': 'Jane', 'age': 24}])
[1, 2, 3]
>>> ^D
```

```sh
$ tinydb-query db.json '{"name": {"$re": ".*n$"}, "age": {"$lt": 30}}'
[{'name': 'John', 'age': 22}]
1 document found.
```

## Commandline help

```
positional arguments:
  db_path               input db (default: "-" read from stdin)
  query                 DB query

optional arguments:
  -h, --help            show this help message and exit
  --schema              show a query-language jsonschema
  --max-depth MAX_DEPTH
                        maximum depth to show (a value <= 0 means unlimited)
  --with-index          display as an indexed dictionary
  --sample N            sample N documents randomly
  --json                output as a JSON text
```