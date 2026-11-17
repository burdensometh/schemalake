from pathlib import Path
import argparse
import json
import sys

from .app import Lake
from .types import LakeError, canonical


def json_file(path):
    with Path(path).open(encoding='utf-8') as stream:
        return json.load(stream)


def parser():
    result = argparse.ArgumentParser(description='Operate the SchemaLake data ingestion engine')
    result.add_argument('--home', default='var/lake')
    commands = result.add_subparsers(dest='command', required=True)
    create = commands.add_parser('create')
    create.add_argument('table')
    create.add_argument('schema_file')
    commands.add_parser('tables')
    ingest = commands.add_parser('ingest')
    ingest.add_argument('table')
    ingest.add_argument('batch_id')
    ingest.add_argument('records_file')
    ingest.add_argument('--invalid', choices=['reject', 'quarantine'], default='reject')
    ingest.add_argument('--expected-head', type=int)
    load = commands.add_parser('import')
    load.add_argument('table')
    load.add_argument('import_id')
    load.add_argument('input_file')
    load.add_argument('--format', choices=['jsonl', 'csv'], default='jsonl')
    for operation in ['rows', 'plan', 'statistics', 'partitions', 'snapshots', 'check', 'index', 'history']:
        command = commands.add_parser(operation)
        command.add_argument('table')
        if operation in {'rows', 'plan', 'statistics', 'partitions', 'index'}:
            command.add_argument('--snapshot', type=int)
    replace = commands.add_parser('replace')
    replace.add_argument('table')
    replace.add_argument('records_file')
    replace.add_argument('--partition-file')
    replace.add_argument('--expected-head', type=int)
    compact = commands.add_parser('compact')
    compact.add_argument('table')
    compact.add_argument('--target-rows', type=int, default=10000)
    compact.add_argument('--partition-file')
    query = commands.add_parser('query')
    query.add_argument('table')
    query.add_argument('predicate_file')
    query.add_argument('--snapshot', type=int)
    lookup = commands.add_parser('lookup')
    lookup.add_argument('table')
    lookup.add_argument('key_file')
    lookup.add_argument('--snapshot', type=int)
    export = commands.add_parser('export')
    export.add_argument('table')
    export.add_argument('--snapshot', type=int)
    export.add_argument('--format', choices=['jsonl', 'csv'], default='jsonl')
    export.add_argument('--predicate-file')
    quality = commands.add_parser('quality')
    quality.add_argument('table')
    quality.add_argument('rules_file')
    quality.add_argument('--snapshot', type=int)
    transform = commands.add_parser('transform-add')
    transform.add_argument('job')
    transform.add_argument('source')
    transform.add_argument('target')
    transform.add_argument('definition_file')
    for operation in ['refresh', 'preview-transform']:
        command = commands.add_parser(operation)
        command.add_argument('job')
        command.add_argument('--snapshot', type=int)
    route = commands.add_parser('route')
    route.add_argument('request_file')
    commands.add_parser('inventory')
    serve = commands.add_parser('serve')
    serve.add_argument('--host', default='127.0.0.1')
    serve.add_argument('--port', type=int, default=8762)
    return result


def execute(lake, args):
    operation = args.command
    if operation == 'create':
        return lake.catalog.create_table(args.table, json_file(args.schema_file))
    if operation == 'tables':
        return lake.catalog.tables()
    if operation == 'ingest':
        return lake.ingest.ingest(args.table, args.batch_id, json_file(args.records_file), args.invalid, args.expected_head)
    if operation == 'import':
        return lake.imports.load(args.table, args.import_id, args.input_file, args.format)
    if operation == 'rows':
        return list(lake.scan.rows(args.table, args.snapshot))
    if operation == 'plan':
        return lake.scan.plan(args.table, args.snapshot)
    if operation == 'statistics':
        return lake.scan.statistics(args.table, args.snapshot)
    if operation == 'partitions':
        return lake.maintenance.partitions(args.table, args.snapshot)
    if operation == 'snapshots':
        return lake.catalog.snapshots(args.table)
    if operation == 'check':
        return lake.diagnostics.check(args.table)
    if operation == 'history':
        return lake.diagnostics.history(args.table)
    if operation == 'index':
        return lake.indexes.build(args.table, args.snapshot)
    if operation == 'lookup':
        return lake.indexes.lookup(args.table, json_file(args.key_file), args.snapshot)
    if operation == 'replace':
        partition = json_file(args.partition_file) if args.partition_file else None
        return lake.maintenance.replace(args.table, json_file(args.records_file), partition, args.expected_head)
    if operation == 'compact':
        partition = json_file(args.partition_file) if args.partition_file else None
        return lake.maintenance.compact(args.table, partition, args.target_rows)
    if operation == 'query':
        return list(lake.scan.where(args.table, json_file(args.predicate_file), args.snapshot))
    if operation == 'export':
        expression = json_file(args.predicate_file) if args.predicate_file else None
        return lake.exports.create(args.table, args.snapshot, args.format, expression=expression)
    if operation == 'quality':
        return lake.quality.evaluate(args.table, json_file(args.rules_file), args.snapshot)
    if operation == 'transform-add':
        return lake.transforms.register(args.job, args.source, args.target, json_file(args.definition_file))
    if operation == 'refresh':
        return lake.transforms.refresh(args.job, args.snapshot)
    if operation == 'preview-transform':
        return lake.transforms.preview(args.job, args.snapshot)
    if operation == 'route':
        return lake.routing.route(**json_file(args.request_file))
    if operation == 'inventory':
        return lake.diagnostics.inventory()
    if operation == 'serve':
        from .api import server
        http = server(lake, args.host, args.port)
        try:
            http.serve_forever()
        finally:
            http.server_close()
        return None
    raise ValueError('unrecognized operation')


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        value = execute(Lake(args.home), args)
        if value is not None:
            print(canonical(value))
        return 0
    except (LakeError, OSError, ValueError) as error:
        print(str(error), file=sys.stderr)
        return 1
