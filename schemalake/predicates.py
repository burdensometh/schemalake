from decimal import Decimal

from .types import Invalid, convert


class Predicate:
    def __init__(self, schema, specification):
        self.fields = {field.name: field for field in schema.fields}
        self.tree = self.compile(specification)

    def compile(self, spec):
        if not isinstance(spec, dict):
            raise Invalid('predicate must be an object')
        op = spec.get('op')
        if op in {'and', 'or'}:
            args = spec.get('args')
            if not isinstance(args, list) or not args:
                raise Invalid('logical predicate needs nonempty args')
            return op, tuple(self.compile(arg) for arg in args)
        if op == 'not':
            return op, self.compile(spec.get('arg'))
        column = spec.get('field')
        if column not in self.fields:
            raise Invalid('predicate references unknown field')
        if op in {'is_null', 'not_null'}:
            return op, column
        if op not in {'eq', 'ne', 'lt', 'le', 'gt', 'ge', 'in'}:
            raise Invalid('unknown predicate operation')
        field = self.fields[column]
        if op == 'in':
            values = spec.get('values')
            if not isinstance(values, list):
                raise Invalid('in predicate requires values array')
            value = tuple(None if v is None else convert(v, field.type) for v in values)
        else:
            if 'value' not in spec:
                raise Invalid('comparison requires value')
            value = None if spec['value'] is None else convert(spec['value'], field.type)
        return op, column, value

    def scalar(self, column, value):
        if value is None:
            return None
        return Decimal(value) if self.fields[column].type == 'decimal' else value

    def evaluate(self, row, tree=None):
        tree = self.tree if tree is None else tree
        op = tree[0]
        if op in {'and', 'or'}:
            results = [self.evaluate(row, item) for item in tree[1]]
            if op == 'and':
                return False if False in results else (None if None in results else True)
            return True if True in results else (None if None in results else False)
        if op == 'not':
            result = self.evaluate(row, tree[1])
            return None if result is None else not result
        column = tree[1]
        value = self.scalar(column, row[column])
        if op == 'is_null':
            return value is None
        if op == 'not_null':
            return value is not None
        if value is None:
            return None
        if op == 'in':
            other = tuple(self.scalar(column, v) for v in tree[2])
            return True if value in other else (None if None in other else False)
        other = self.scalar(column, tree[2])
        if other is None:
            return None
        if op == 'eq':
            return value == other
        if op == 'ne':
            return value != other
        if op == 'lt':
            return value < other
        if op == 'le':
            return value <= other
        if op == 'gt':
            return value > other
        return value >= other

    def matches(self, row):
        return self.evaluate(row) is True

    def columns(self, tree=None):
        tree = self.tree if tree is None else tree
        if tree[0] in {'and', 'or'}:
            result = set()
            for item in tree[1]:
                result.update(self.columns(item))
            return result
        if tree[0] == 'not':
            return self.columns(tree[1])
        return {tree[1]}
