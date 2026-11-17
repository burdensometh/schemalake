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

    def may_match_file(self, file):
        """False only when the object's own statistics prove that no stored row
        can satisfy this predicate; any doubt keeps the file readable."""
        return True in self.file_outcomes(file)

    def file_outcomes(self, file, tree=None):
        """Superset of the three-valued outcomes this predicate can take over
        the rows of one data object, derived from manifest statistics only.
        Every combinator rule mirrors evaluate() so that a real outcome is
        never dropped: True absent here means absent for every stored row."""
        tree = self.tree if tree is None else tree
        op = tree[0]
        if op in {'and', 'or'}:
            children = [self.file_outcomes(file, item) for item in tree[1]]
            outcomes = set()
            if op == 'and':
                if all(True in child for child in children):
                    outcomes.add(True)
                if any(False in child for child in children):
                    outcomes.add(False)
                if any(None in child for child in children) and all(child - {False} for child in children):
                    outcomes.add(None)
            else:
                if any(True in child for child in children):
                    outcomes.add(True)
                if all(False in child for child in children):
                    outcomes.add(False)
                if any(None in child for child in children) and all(child - {True} for child in children):
                    outcomes.add(None)
            return outcomes
        if op == 'not':
            flip = {True: False, False: True, None: None}
            return {flip[outcome] for outcome in self.file_outcomes(file, tree[1])}
        return self.leaf_outcomes(file, tree)

    def leaf_outcomes(self, file, tree):
        op, column = tree[0], tree[1]
        unknown = {True, False, None}
        statistics = file.get('statistics') if isinstance(file, dict) else None
        if not isinstance(statistics, dict):
            return unknown
        entry = statistics.get(column)
        if not isinstance(entry, dict):
            return unknown
        null_count, row_count = entry.get('null_count'), file.get('row_count')
        if type(null_count) is not int or type(row_count) is not int or not 0 <= null_count <= row_count:
            return unknown
        has_nulls = null_count > 0
        has_values = null_count < row_count
        if op == 'is_null':
            return ({True} if has_nulls else set()) | ({False} if has_values else set())
        if op == 'not_null':
            return ({False} if has_nulls else set()) | ({True} if has_values else set())
        outcomes = set()
        if has_nulls:
            outcomes.add(None)
        if not has_values:
            return outcomes
        if op != 'in' and tree[2] is None:
            outcomes.add(None)
            return outcomes
        low, high = entry.get('min'), entry.get('max')
        if low is None or high is None:
            return unknown
        low, high = self.scalar(column, low), self.scalar(column, high)
        if op == 'in':
            candidates = [self.scalar(column, value) for value in tree[2] if value is not None]
            if any(low <= candidate <= high for candidate in candidates):
                outcomes.add(True)
            if not (low == high and low in candidates):
                outcomes.add(None if any(value is None for value in tree[2]) else False)
            return outcomes
        target = self.scalar(column, tree[2])
        below, above = low < target, high > target
        equal = low <= target <= high
        if op == 'eq':
            possible, impossible = equal, below or above
        elif op == 'ne':
            possible, impossible = below or above, equal
        elif op == 'lt':
            possible, impossible = below, high >= target
        elif op == 'le':
            possible, impossible = low <= target, above
        elif op == 'gt':
            possible, impossible = above, low <= target
        else:
            possible, impossible = high >= target, below
        if possible:
            outcomes.add(True)
        if impossible:
            outcomes.add(False)
        return outcomes

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
