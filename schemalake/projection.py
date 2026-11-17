from .types import Conflict, Invalid, convert


# 唯一允许的类型拓宽：整数测量值扩为精确小数。
# 两个方向必须互为逆变换：int64 写入的整数转成 decimal 字符串后，
# 历史快照按 int64 读取时仍能还原同一个整数。
WIDENINGS = {('int64', 'decimal')}


def field_index(schema):
    return {field.id: field for field in schema.fields}


def widen(value, from_type, to_type):
    if from_type == to_type:
        return value
    if (from_type, to_type) not in WIDENINGS:
        raise Conflict(f'incompatible type change: {from_type} -> {to_type}')
    if from_type == 'int64' and to_type == 'decimal':
        if type(value) is not int:
            raise Conflict('stored int64 value is not an integer')
        return convert(value, 'decimal')
    raise Conflict(f'incompatible type change: {from_type} -> {to_type}')


def project_row(raw, source_schema, target_schema):
    """把按 source_schema 写入的一行投影到 target_schema。

    字段身份以 field id 为准：改名按 id 延续，新增列取默认值/空值，
    允许的类型拓宽逐值精确转换。主键与分区列不允许演进，因此其名称与
    取值在两个 schema 间保持稳定。
    """
    if not isinstance(raw, dict):
        raise Invalid('data object row must be an object')
    source_by_id = field_index(source_schema)
    source_by_name = {field.name: field for field in source_schema.fields}
    result = {}
    for target in target_schema.fields:
        source = source_by_id.get(target.id)
        if source is None:
            if target.name in raw:
                # 新增字段的 id 不可能与旧字段重名却缺失 id 映射，防御性处理。
                raise Conflict(f'row carries value for unknown field id {target.id}')
            if target.has_default:
                result[target.name] = target.default
            elif target.nullable:
                result[target.name] = None
            else:
                raise Conflict(f'field id {target.id} has no value and no default')
            continue
        old_value = raw.get(source.name)
        if old_value is None:
            if not target.nullable:
                raise Conflict(f'null in nonnullable column: {target.name}')
            result[target.name] = None
            continue
        if source.type != target.type:
            old_value = widen(old_value, source.type, target.type)
        else:
            # 同类型仅做规范化，保证不同版本读出的等值形式一致。
            old_value = convert(old_value, target.type)
        result[target.name] = old_value
    return result


def project_schema_rows(rows, source_schema, target_schema):
    for raw in rows:
        yield project_row(raw, source_schema, target_schema)
