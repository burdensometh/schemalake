from decimal import Decimal, InvalidOperation, localcontext

from .types import Invalid


class Expression:
    def __init__(self, specification, columns):
        self.columns = set(columns)
        self.tree = self.compile(specification)

    def compile(self, spec):
        if not isinstance(spec, dict):
            raise Invalid('expression must be an object')
        if set(spec) == {'field'}:
            if spec['field'] not in self.columns:
                raise Invalid('expression references unknown column')
            return 'field', spec['field']
        if set(spec) == {'literal'}:
            value = spec['literal']
            if value is not None and type(value) not in {str, int, bool}:
                raise Invalid('unsupported expression literal')
            return 'literal', value
        op = spec.get('op')
        args = spec.get('args')
        arity = {'add': 2, 'subtract': 2, 'multiply': 2, 'divide': 2,
                 'lower': 1, 'upper': 1, 'concat': 2, 'coalesce': 2}
        if op not in arity or not isinstance(args, list) or len(args) != arity[op]:
            raise Invalid('invalid expression operation or arity')
        return op, tuple(self.compile(arg) for arg in args)

    def numeric(self, value):
        if type(value) not in {str, int}:
            raise Invalid('numeric expression needs integer or decimal string')
        try:
            number = Decimal(value)
        except InvalidOperation:
            raise Invalid('invalid numeric expression operand') from None
        if not number.is_finite():
            raise Invalid('nonfinite expression operand')
        return number

    def evaluate(self, row, tree=None):
        tree = self.tree if tree is None else tree
        op = tree[0]
        if op == 'field':
            return row[tree[1]]
        if op == 'literal':
            return tree[1]
        args = [self.evaluate(row, arg) for arg in tree[1]]
        if op == 'coalesce':
            return args[0] if args[0] is not None else args[1]
        if None in args:
            return None
        if op in {'lower', 'upper', 'concat'}:
            if any(not isinstance(value, str) for value in args):
                raise Invalid('text expression requires strings')
            if op == 'lower':
                return args[0].lower()
            if op == 'upper':
                return args[0].upper()
            return args[0] + args[1]
        left, right = map(self.numeric, args)
        with localcontext() as context:
            context.prec = 80
            if op == 'add':
                result = left + right
            elif op == 'subtract':
                result = left - right
            elif op == 'multiply':
                result = left * right
            else:
                if right == 0:
                    raise Invalid('division by zero in transform')
                result = left / right
        return format(result, 'f')
