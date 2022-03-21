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
usage: tinydb-query [-h] [--schema] [--table TABLE] [--max-depth MAX_DEPTH] [--with-index]
                    [--sample N] [--json]
                    [db_path] [query]

positional arguments:
  db_path               input db
  query                 DB query (JSON formatted)

optional arguments:
  -h, --help            show this help message and exit
  --schema              show a query-language jsonschema
  --table TABLE         target table (default: the default table)
  --max-depth MAX_DEPTH
                        maximum depth to show (a value <= 0 means unlimited)
  --with-index          display as an indexed dictionary
  --sample N            sample N documents randomly
  --json                output as a JSON text
```

## Query commands

### Basic query commands
|       Query         | TinyDB |
|---------------------|---------|
| `{"field.name": ...}`<br>`{"field": {"name": ...}}`| `Query().field.name` |
| `{"$and": [qry, ...]}` | `field.qry & ...` |
| `{"$or": [qry, ...]}` | `field.qry \| ...` |
| `{"$not": qry}` | `~field.qry` |
| `{"$exists": boolean}`| `field.exists()` |
| `{"$search": regexp}`<br>`{"$re": regexp}` | `field.search(regexp)` |
| `{"$matches": regexp}`| `field.matches(regexp)` |
| `{"$fragment": {k:v, ...}}`| `field.fragment({k:v, ...})` |
| `{"$any": array-or-qry}` | `field.any(array-or-qry)` |
| `{"$all": array-or-qry}` | `field.all(array-or-qry)` |
| `{"$enum": [item, ...]}`| `field.one_of([item, ...])` |
| `{"$eq": value}`,<br>`string`, `number`, `boolean` values| `field == value` |
| `{"$ne": value}` | `field != value` |
| `{"$ge": value}`, `{"$gt": value}` | `field >= value`, `field > value` |
| `{"$le": value}`, `{"$lt": value}` | `field <= value`, `field < value` |

### Extended commands
|       Query         |         |
|---------------------|---------|
| `{"$length": qry}`  | Select a document when the len(field) satisfies `qry` |
| `{"$types": [type, ...]}`<br>`types`: JSON types| Select a document when the field type is in `[type, ...]` |

## Examples
```{"address.country": "Japan", "age": 20}```
```python
(Query().address.country == "Japan") & (Query().age == 20)
```

```{"course": {"$all": {"score": {"$gt": 80}}}}```
```python
Query().course.all(Query().score > 80)
```