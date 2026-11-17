from pathlib import Path

from .catalog import Catalog
from .storage import Objects
from .ingestion import BatchIngestor
from .scan import Scanner
from .maintenance import Maintenance
from .indexes import KeyIndexes
from .planning import Plans
from .transforms import Transforms
from .routing import RoutedIngestor
from .joins import SnapshotJoin
from .quality import Quality
from .exports import Exports
from .imports import FileImports
from .diagnostics import Diagnostics
from .stream_imports import StreamImports
from .aggregations import AggregateJobs
from .rowdiff import RowDiff
from .sorting import SortedRows
from .encoded_storage import EncodedObjects
from .exchange import Exchange
from .staging import StagedIngestor
from .reconcile import Reconcile
from .enrichment import Enrichment
from .provenance import Provenance


class Lake:
    def __init__(self, home, file_rows=1000, encoding='jsonl'):
        self.home = Path(home).resolve()
        self.home.mkdir(parents=True, exist_ok=True)
        self.catalog = Catalog(self.home)
        self.objects = EncodedObjects(self.home / 'objects', encoding)
        self.ingest = BatchIngestor(self.catalog, self.objects, file_rows)
        self.scan = Scanner(self.catalog, self.objects)
        self.maintenance = Maintenance(self.catalog, self.objects)
        self.indexes = KeyIndexes(self.catalog, self.objects, self.scan)
        self.plans = Plans(self.catalog, self.scan)
        self.transforms = Transforms(self.catalog, self.scan, self.maintenance)
        self.routing = RoutedIngestor(self.catalog, self.ingest)
        self.joins = SnapshotJoin(self.catalog, self.scan)
        self.quality = Quality(self.catalog, self.scan)
        self.exports = Exports(self.catalog, self.scan, self.home / 'exports')
        self.imports = FileImports(self.catalog, self.ingest)
        self.diagnostics = Diagnostics(self.catalog, self.objects, self.scan)
        self.stream_imports = StreamImports(self.catalog, self.ingest)
        self.aggregates = AggregateJobs(self.catalog, self.scan, self.maintenance)
        self.rowdiff = RowDiff(self.catalog, self.scan)
        self.sort = SortedRows(self.catalog, self.scan)
        self.exchange = Exchange(self.catalog, self.objects, self.scan, self.ingest)
        self.staged = StagedIngestor(self.catalog, self.objects, self.ingest)
        self.reconcile = Reconcile(self.catalog, self.scan)
        self.enrichment = Enrichment(self.catalog, self.joins, self.maintenance)
        self.provenance = Provenance(self.catalog, self.objects)
