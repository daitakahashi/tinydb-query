import functools
import numbers
import operator
import re
from collections.abc import Sized
from collections import deque

import jsonschema
import tinydb

# pylint: disable = too-few-public-methods

class LoadError(TypeError):
    pass


class QLSyntaxError(ValueError):
    pass


def _any_of(candidates, *args, **kwargs):
    errors = []
    for cand in candidates:
        try:
            return cand(*args, **kwargs)
        except LoadError as exc:
            errors.append(exc)
    raise LoadError(errors)


typename2datatype = {
    'string': str,
    'boolean': bool,  # in python, bool is a subset of int
    'number': numbers.Number,
    'array': list,
    'object': dict
}


def ident(data):
    return data


def get_referenced_class():
    def _collect_subclass(cls):
        available_classes[cls.__name__] = cls
        for _cls in cls.__subclasses__():
            _collect_subclass(_cls)
    available_classes = {}
    _collect_subclass(ParsedObject)
    return available_classes


class LoadAll:
    @staticmethod
    def load(data):
        return data


class LoadAnyOf:
    def __init__(self, spec):
        self._spec = [
            Loader(elem) for elem in spec['anyOf']
        ]

    def load(self, data):
        return _any_of([
            elem.load for elem in self._spec
        ], data)


class LoadRef:
    def __init__(self, spec):
        available_classes = get_referenced_class()
        self._ref = available_classes[spec['$ref']]

    def load(self, data):
        return self._ref(data)


class LoadData:
    # pylint: disable = too-many-instance-attributes
    def __init__(self, spec):
        self.spec = spec
        if 'type' in spec:
            expected_type = typename2datatype[spec['type']]
            self._typecheck = (
                lambda x: isinstance(x, expected_type)
            )
        else:
            self._typecheck = lambda x: True
        self._properties = {
            key: Loader(value)
            for key, value in spec.get('properties', {}).items()
        }
        self._pattern_properties = [
            (re.compile(key), Loader(value))
            for key, value in spec.get('patternProperties', {}).items()
        ]
        required = set(spec.get('required', []))
        extra_ok = spec.get('additionalProperties', True)
        names_in_spec = set(self._properties)
        patterns_in_spec = list(
            regex for regex, _ in self._pattern_properties
        )
        enum_values = spec.get('enum')

        def check_names(data):
            if not isinstance(data, dict):
                return True
            names = set(data)
            if required - names:
                return False
            if not extra_ok:
                remaining = names - names_in_spec
                remaining = [
                    name for name in remaining
                    if any(not rx.match(name) for rx in patterns_in_spec)
                ]
                return not remaining
            return True

        def check_enums(data):
            if enum_values is None:
                return True
            return data in enum_values

        self._property_names_ok = check_names
        self._enum_values_ok = check_enums
        self._additional_items = spec.get('additionalItems', True)
        self._array_elem_loader = Loader(spec.get('items', {}))

    def load(self, data):
        if not self._typecheck(data):
            raise LoadError()
        if not self._enum_values_ok(data):
            raise LoadError()
        if not self._property_names_ok(data):
            raise LoadError()
        if isinstance(data, list):
            result = []
            for elem in data:
                try:
                    result.append(self._array_elem_loader.load(elem))
                except LoadError as exc:
                    if self._additional_items:
                        result.append(elem)
                    else:
                        raise exc
            return result
        if isinstance(data, dict):
            def _load_dictelem(key, value):
                if key in self._properties:
                    result[key] = self._properties[key].load(value)
                    return
                for regex, loader in self._pattern_properties:
                    if regex.match(key):
                        result[key] = loader.load(value)
                        return
                result[key] = value

            result = {}
            for key, value in data.items():
                _load_dictelem(key, value)
            return result
        return data


class Loader:
    def __init__(self, spec):
        if spec == {}:
            self._loader = LoadAll()
            return
        if 'anyOf' in spec:
            self._loader = LoadAnyOf(spec)
            return
        if '$ref' in spec:
            self._loader = LoadRef(spec)
            return
        self._loader = LoadData(spec)

    def load(self, data):
        return self._loader.load(data)


def spec_to_schema(spec):
    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema"
    }
    defs = {}
    available_classes = get_referenced_class()
    ref_targets = deque()

    def _evaluate_ref(target):
        cls = available_classes[target]
        if target not in defs:
            def resolve_ref():
                defs[target] = None  # to stop recursion
                defs[target] = _spec_to_schema(cls.spec)
            ref_targets.append(resolve_ref)
        return f'#/$defs/{target}'

    def _spec_to_schema(spec):
        if isinstance(spec, dict):
            result = {}
            for key, value in spec.items():
                if key == '$ref':
                    result[key] = _evaluate_ref(value)
                else:
                    result[key] = _spec_to_schema(value)
        elif isinstance(spec, list):
            result = [_spec_to_schema(elem) for elem in spec]
        else:
            result = spec
        return result

    result = _spec_to_schema(spec)
    while ref_targets:
        ref_targets.popleft()()
    if defs:
        schema['$defs'] = dict(reversed(defs.items()))
    schema.update(result)
    return schema


class ParsedObject:
    spec = {}
    loader = None
    schema = None

    def __init__(self, data):
        if self.__class__.loader is None:
            self.__class__.loader = Loader(self.spec)
        self.data = data
        self.value = self.loader.load(data)

    @classmethod
    def get_schema(cls):
        if cls.schema is None:
            try:
                cls.schema = spec_to_schema({'$ref': cls.__name__})
            except KeyError as exc:
                raise LoadError(str(exc)) from exc
        return cls.schema

    def render(self, current):
        return self.value.render(current)


class String(ParsedObject):
    spec = {
        "$comment": "simple datatype: string",
        "type": "string"
    }

    def render(self, _):
        return self.value


class Boolean(ParsedObject):
    spec = {
        "$comment": "simple datatype: boolean",
        "type": "boolean"
    }

    def render(self, _):
        return self.value


class Number(ParsedObject):
    spec = {
        "$comment": "simple datatype: number",
        "type": "number"
    }

    def render(self, _):
        return self.value


class Regex(ParsedObject):
    spec = {
        "$comment": "$re: regular expression",
        "type": "object",
        "properties": {"$re": {"$ref": "String"}},
        "required": ["$re"]
    }

    def render(self, current):
        return self.value["$re"].render(current)


class DataList(ParsedObject):
    spec = {
        "$comment": "symple container: list",
        "type": "array"
    }

    def render(self, _):
        return self.value


class Exists(ParsedObject):
    spec = {
        "$comment": "$exists: matches any document with the field; query.exists()",
        "type": "object",
        "properties": {"$exists": {"$ref": "Boolean"}},
        "required": ["$exists"],
        "additionalProperties": False
    }

    def render(self, current):
        if self.value["$exists"].value:
            return current.exists()
        return ~current.exists()


class Matches(ParsedObject):
    spec = {
        "$comment": "$matches: match any document with the whole field "
        "matching the regexp; query.matches(arg)",
        "type": "object",
        "properties": {
            "$matches": {"anyOf": [
                {"$ref": "Regex"},
                {"$ref": "String"}
            ]}
        },
        "required": ["$matches"],
        "additionalProperties": False
    }

    def render(self, current):
        return current.matches(
            self.value["$matches"].render(current)
        )


class Search(ParsedObject):
    spec = {
        "$comment": "$search: match any document with a substring of the field "
        "matching the regexp; query.search(arg)",
        "type": "object",
        "properties": {
            "$search": {"anyOf": [
                {"$ref": "Regex"},
                {"$ref": "String"}
            ]}
        },
        "required": ["$search"],
        "additionalProperties": False
    }

    def render(self, current):
        return current.search(
            self.value["$search"].render(current)
        )


class Fragment(ParsedObject):
    spec = {
        "$comment": "$fragment: match with a document fragment; query.fragment({...})",
        "type": "object",
        "properties": {
            "$fragment": {
                "type": "object"
            }
        },
        "required": ["$fragment"],
        "additionalProperties": False
    }

    def render(self, current):
        return current.fragment(self.value["$fragment"])


class Types(ParsedObject):
    spec = {
        "$comment": "$types: match by the document's datatype; no tinydb correspondence",
        "type": "object",
        "properties": {
            "$types": {
                "type": "array",
                "items": {
                    "type": "string",
                    "enum": list(typename2datatype)
                }
            }
        },
        "required": ["$types"],
        "additionalProperties": False
    }

    def render(self, current):
        allowed_types = tuple(
            typename2datatype[name] for name in self.value["$types"]
        )
        return current.test(lambda x: isinstance(x, allowed_types))


class Enum(ParsedObject):
    spec = {
        "$comment": "$enum: match if the field is contained in the list; "
        "query.one_of([...])",
        "type": "object",
        "properties": {
            "$enum": {"$ref": "DataList"}
        },
        "required": ["$enum"],
        "additionalProperties": False
    }

    def render(self, current):
        return current.one_of(
            self.value["$enum"].render(current)
        )


class Length(ParsedObject):
    spec = {
        "$comment": "$length: match if the length of the document satisfies condition; "
        "no tinydb correspondence",
        "type": "object",
        "properties": {
            "$length": {"anyOf": [
                {"$ref": "Verb"},
                {"$ref": "DefaultEq"}
            ]}
        },
        "required": ["$length"],
        "additionalProperties": False
    }

    def render(self, current):
        following_query = self.value["$length"].render(
            tinydb.Query().map(len)
        )
        return current.test(
            lambda data: (
                isinstance(data, Sized) and following_query(data)
            )
        )


class DefaultEq(ParsedObject):
    spec = {
        "$comment": "implicit == comparison for a literal atom",
        "anyOf": [
            {"$ref": "Number"},
            {"$ref": "Boolean"},
            {"$ref": "String"}
        ]
    }

    def render(self, current):
        return current == self.value.render(current)


class DefaultSearch(ParsedObject):
    spec = {
        "$comment": "implicit query.search() for regexp",
        "$ref": "Regex"
    }

    def render(self, current):
        return current.search(self.value.render(current))


class Eq(ParsedObject):
    spec = {
        "$comment": "match if the document is equal to the given value; ==",
        "type": "object",
        "properties": {
            "$eq": {}
        },
        "required": ["$eq"],
        "additionalProperties": False
    }

    def render(self, current):
        return current == self.value["$eq"]


class Ne(ParsedObject):
    spec = {
        "$comment": "match if the document exists and is unequal to the given value; !=",
        "type": "object",
        "properties": {
            "$ne": {}
        },
        "required": ["$ne"],
        "additionalProperties": False
    }

    def render(self, current):
        return current != self.value["$ne"]


class Lt(ParsedObject):
    spec = {
        "$comment": "match if the document is less than the given value; <",
        "type": "object",
        "properties": {
            "$lt": {}
        },
        "required": ["$lt"],
        "additionalProperties": False
    }

    def render(self, current):
        return current < self.value["$lt"]


class Le(ParsedObject):
    spec = {
        "$comment": "match if the document is less or equal to than the given value; <=",
        "type": "object",
        "properties": {
            "$le": {}
        },
        "required": ["$le"],
        "additionalProperties": False
    }

    def render(self, current):
        return current <= self.value["$le"]


class Gt(ParsedObject):
    spec = {
        "$comment": "match if the document is greater than the given value; >",
        "type": "object",
        "properties": {
            "$gt": {}
        },
        "required": ["$gt"],
        "additionalProperties": False
    }

    def render(self, current):
        return current > self.value["$gt"]


class Ge(ParsedObject):
    spec = {
        "$comment": "match if the document is greater than or equal to the given value; >=",
        "type": "object",
        "properties": {
            "$ge": {}
        },
        "required": ["$ge"],
        "additionalProperties": False
    }

    def render(self, current):
        return current >= self.value["$ge"]


class Compare(ParsedObject):
    spec = {
        "$comment": "comparison operators",
        "anyOf": [
            {"$ref": "Eq"},
            {"$ref": "Ne"},
            {"$ref": "Lt"},
            {"$ref": "Le"},
            {"$ref": "Gt"},
            {"$ref": "Ge"}
        ]
    }


class And(ParsedObject):
    spec = {
        "type": "object",
        "properties": {
            "$and": {
                "type": "array",
                "items": {"anyOf": [
                    {"$ref": "Verb"},
                    {"$ref": "DefaultEq"},
                    {"$ref": "DefaultSearch"}
                ]},
                "additionalItems": False
            }
        },
        "required": ["$and"],
        "additionalProperties": False
    }

    def render(self, current):
        if not self.value["$and"]:
            return current.noop()
        return functools.reduce(
            operator.and_, (
                elem.render(current) for elem in self.value["$and"]
            )
        )


class Or(ParsedObject):
    spec = {
        "type": "object",
        "properties": {
            "$or": {
                "type": "array",
                "items": {"anyOf": [
                    {"$ref": "Verb"},
                    {"$ref": "DefaultEq"},
                    {"$ref": "DefaultSearch"}
                ]},
                "additionalItems": False
            }
        },
        "required": ["$or"],
        "additionalProperties": False
    }

    def render(self, current):
        if not self.value["$or"]:
            return ~current.noop()
        return functools.reduce(
            operator.or_, (
                elem.render(current) for elem in self.value["$or"]
            )
        )


class Not(ParsedObject):
    spec = {
        "type": "object",
        "properties": {
            "$not": {"anyOf": [
                    {"$ref": "Verb"},
                    {"$ref": "DefaultEq"},
                    {"$ref": "DefaultSearch"}
            ]}
        },
        "required": ["$not"],
        "additionalProperties": False
    }

    def render(self, current):
        return ~self.value["$not"].render(current)


class All(ParsedObject):
    spec = {
        "$comment": "$all: select if all documents in the list field "
        "(1) match the query, or (2) are a member of the given list; query.all([...])",
        "type": "object",
        "properties": {
            "$all": {"anyOf": [
                {"$ref": "Verb"},
                {"$ref": "DefaultEq"},
                {"$ref": "DefaultSearch"},
                {"$ref": "DataList"}
            ]}
        },
        "required": ["$all"],
        "additionalProperties": False
    }

    def render(self, current):
        try:
            inner = self.value["$all"].render(tinydb.Query())
        except ValueError:
            # assign a trivial mapping when the path is empty;
            # allowing {"a": {"$all": {"$gt": 12}}} to match an array of which
            # all elements are greater than 12.
            inner = self.value["$all"].render(tinydb.Query().map(ident))
        return current.all(inner)


class Any(ParsedObject):
    spec = {
        "$comment": "$any: select if at least one document in the list field "
        "(1) match the query, or (2) are a member of the given list; query.any([...])",
        "type": "object",
        "properties": {
            "$any": {"anyOf": [
                {"$ref": "Verb"},
                {"$ref": "DefaultEq"},
                {"$ref": "DefaultSearch"},
                {"$ref": "DataList"}
            ]}
        },
        "required": ["$any"],
        "additionalProperties": False
    }

    def render(self, current):
        try:
            inner = self.value["$any"].render(tinydb.Query())
        except ValueError:
            # assign a trivial mapping when the path is empty;
            # allowing {"a": {"$any": {"$gt": 12}}} to match an array of which
            # at least one element is greater than 12.
            inner = self.value["$any"].render(tinydb.Query().map(ident))
        return current.any(inner)


class Verb(ParsedObject):
    spec = {
        "anyOf": [
            {"$ref": "Exists"},
            {"$ref": "Matches"},
            {"$ref": "Search"},
            {"$ref": "Fragment"},
            {"$ref": "Types"},
            {"$ref": "Any"},
            {"$ref": "All"},
            {"$ref": "Length"},
            {"$ref": "Enum"},
            {"$ref": "Compare"},
            {"$ref": "And"},
            {"$ref": "Or"},
            {"$ref": "Not"},
            {"$ref": "Field"}
        ]
    }


class Field(ParsedObject):
    spec = {
        "type": "object",
        "patternProperties": {
            "(^[^$.\\s\\d][^$.\\s]*(\\.[^$.\\s\\d][^$.\\s]*)*$)": {
                "anyOf": [
                    {"$ref": "Verb"},
                    {"$ref": "DefaultEq"},
                    {"$ref": "DefaultSearch"}
                ]
            }
        },
        "additionalProperties": False
    }

    def render(self, current):
        queries = []
        if not self.value:  # i.e., value == {}
            return current.noop()

        for key, value in self.value.items():
            query = current
            fields = key.split(".")
            for field in fields:
                query = getattr(query, field)
            queries.append(value.render(query))
        if len(queries) == 1:
            return queries[0]
        return functools.reduce(operator.and_, queries)


class TopLevelAnd(And):
    spec = {
        "type": "object",
        "properties": {
            "$and": {
                "type": "array",
                "items": {"$ref": "TopLevel"},
                "additionalItems": False
            }
        },
        "required": ["$and"],
        "additionalProperties": False
    }


class TopLevelOr(Or):
    spec = {
        "type": "object",
        "properties": {
            "$or": {
                "type": "array",
                "items": {"$ref": "TopLevel"},
                "additionalItems": False
            }
        },
        "required": ["$or"],
        "additionalProperties": False
    }

class TopLevelNot(Not):
    spec = {
        "type": "object",
        "properties": {
            "$not": {"$ref": "TopLevel"}
        },
        "required": ["$not"],
        "additionalProperties": False
    }


class TopLevel(ParsedObject):
    spec = {"anyOf": [
        {"$ref": "Fragment"},
        {"$ref": "TopLevelAnd"},
        {"$ref": "TopLevelOr"},
        {"$ref": "TopLevelNot"},
        {"$ref": "Field"}
    ]}


def Schema(target=TopLevel):
    return target.get_schema()


def Query(query):
    entry_point = TopLevel
    schema = Schema(entry_point)
    try:
        jsonschema.validators.validator_for(schema)(
            schema
        ).validate(query)
    except jsonschema.exceptions.SchemaError as exc:
        raise LoadError(str(exc)) from exc
    except jsonschema.exceptions.ValidationError as exc:
        raise QLSyntaxError(str(exc)) from exc
    return entry_point(query).render(tinydb.Query())
