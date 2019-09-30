import os
import shutil
from dataclasses import dataclass
from datetime import datetime
from typing import Union, Dict, List, Optional, Any, NamedTuple

from hologram import JsonSchemaMixin, ValidationError

from dbt.adapters.factory import get_adapter
from dbt.contracts.graph.compiled import CompileResultNode
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.util import Writable, Replaceable
from dbt.include.global_project import DOCS_INDEX_FILE_PATH
import dbt.ui.printer
import dbt.utils
import dbt.compilation
import dbt.exceptions

from dbt.task.compile import CompileTask


CATALOG_FILENAME = 'catalog.json'


def get_stripped_prefix(source: Dict[str, Any], prefix: str) -> Dict[str, Any]:
    """Go through source, extracting every key/value pair where the key starts
    with the given prefix.
    """
    cut = len(prefix)
    return {
        k[cut:]: v for k, v in source.items()
        if k.startswith(prefix)
    }


Primitive = Union[bool, str, float, None]
PrimitiveDict = Dict[str, Primitive]


Key = NamedTuple(
    'Key',
    [('database', str), ('schema', str), ('name', str)]
)


@dataclass
class StatsItem(JsonSchemaMixin):
    id: str
    label: str
    value: Primitive
    description: str
    include: bool


StatsDict = Dict[str, StatsItem]


@dataclass
class ColumnMetadata(JsonSchemaMixin):
    type: str
    comment: Optional[str]
    index: int
    name: str


ColumnMap = Dict[str, ColumnMetadata]


@dataclass
class TableMetadata(JsonSchemaMixin):
    type: str
    database: str
    schema: str
    name: str
    comment: Optional[str]
    owner: Optional[str]


@dataclass
class Table(JsonSchemaMixin, Replaceable):
    metadata: TableMetadata
    columns: ColumnMap
    stats: StatsDict
    # the same table with two unique IDs will just be listed two times
    unique_id: Optional[str] = None

    @classmethod
    def from_query_result(cls, data) -> 'Table':
        # build the new table's metadata + stats
        metadata = TableMetadata.from_dict(get_stripped_prefix(data, 'table_'))
        stats = format_stats(get_stripped_prefix(data, 'stats:'))

        return cls(
            metadata=metadata,
            stats=stats,
            columns={},
        )

    def key(self) -> Key:
        return Key(
            self.metadata.database.lower(),
            self.metadata.schema.lower(),
            self.metadata.name.lower(),
        )


# keys are database name, schema name, table name
class Catalog(Dict[Key, Table]):
    def __init__(self, columns: List[PrimitiveDict]):
        super().__init__()
        for col in columns:
            self.add_column(col)

    def get_table(self, data: PrimitiveDict) -> Table:
        try:
            key = Key(
                str(data['table_database']),
                str(data['table_schema']),
                str(data['table_name']),
            )
        except KeyError as exc:
            raise dbt.exceptions.CompilationException(
                'Catalog information missing required key {} (got {})'
                .format(exc, data)
            )
        if key in self:
            table = self[key]
        else:
            table = Table.from_query_result(data)
            self[key] = table
        return table

    def add_column(self, data: PrimitiveDict):
        table = self.get_table(data)
        column_data = get_stripped_prefix(data, 'column_')
        # the index should really never be that big so it's ok to end up
        # serializing this to JSON (2^53 is the max safe value there)
        column_data['index'] = int(column_data['index'])

        column = ColumnMetadata.from_dict(column_data)
        table.columns[column.name] = column

    def make_unique_id_map(self, manifest: Manifest) -> Dict[str, Table]:
        nodes: Dict[str, Table] = {}

        manifest_mapping = get_unique_id_mapping(manifest)
        for table in self.values():
            unique_ids = manifest_mapping.get(table.key(), [])
            for unique_id in unique_ids:
                if unique_id in nodes:
                    dbt.exceptions.raise_ambiguous_catalog_match(
                        unique_id, nodes[unique_id].to_dict(), table.to_dict()
                    )
                else:
                    nodes[unique_id] = table.replace(unique_id=unique_id)
        return nodes


def format_stats(stats: PrimitiveDict) -> StatsDict:
    """Given a dictionary following this layout:

        {
            'encoded:label': 'Encoded',
            'encoded:value': 'Yes',
            'encoded:description': 'Indicates if the column is encoded',
            'encoded:include': True,

            'size:label': 'Size',
            'size:value': 128,
            'size:description': 'Size of the table in MB',
            'size:include': True,
        }

    format_stats will convert the dict into a StatsDict with keys of 'encoded'
    and 'size'.
    """
    stats_collector: StatsDict = {}

    base_keys = {k.split(':')[0] for k in stats}
    for key in base_keys:
        dct: PrimitiveDict = {'id': key}
        for subkey in ('label', 'value', 'description', 'include'):
            dct[subkey] = stats['{}:{}'.format(key, subkey)]

        try:
            stats_item = StatsItem.from_dict(dct)
        except ValidationError:
            continue
        if stats_item.include:
            stats_collector[key] = stats_item

    # we always have a 'has_stats' field, it's never included
    has_stats = StatsItem(
        id='has_stats',
        label='Has Stats?',
        value=len(stats_collector) > 0,
        description='Indicates whether there are statistics for this table',
        include=False,
    )
    stats_collector['has_stats'] = has_stats
    return stats_collector


def mapping_key(node: CompileResultNode) -> Key:
    return Key(
        node.database.lower(), node.schema.lower(), node.identifier.lower()
    )


def get_unique_id_mapping(manifest: Manifest) -> Dict[Key, List[str]]:
    # A single relation could have multiple unique IDs pointing to it if a
    # source were also a node.
    ident_map: Dict[Key, List[str]] = {}
    for unique_id, node in manifest.nodes.items():
        key = mapping_key(node)

        if key not in ident_map:
            ident_map[key] = []

        ident_map[key].append(unique_id)
    return ident_map


@dataclass
class CatalogResults(JsonSchemaMixin, Writable):
    nodes: Dict[str, Table]
    generated_at: datetime
    _compile_results: Optional[Any] = None


def _coerce_decimal(value):
    if isinstance(value, dbt.utils.DECIMALS):
        return float(value)
    return value


class GenerateTask(CompileTask):
    def _get_manifest(self) -> Manifest:
        manifest = dbt.loader.GraphLoader.load_all(self.config)
        return manifest

    def run(self):
        compile_results = None
        if self.args.compile:
            compile_results = super().run()
            if any(r.error is not None for r in compile_results):
                dbt.ui.printer.print_timestamped_line(
                    'compile failed, cannot generate docs'
                )
                return CatalogResults({}, datetime.utcnow(), compile_results)

        shutil.copyfile(
            DOCS_INDEX_FILE_PATH,
            os.path.join(self.config.target_path, 'index.html'))

        adapter = get_adapter(self.config)
        with adapter.connection_named('generate_catalog'):
            manifest = self._get_manifest()

            dbt.ui.printer.print_timestamped_line("Building catalog")
            catalog_table = adapter.get_catalog(manifest)

        catalog_data: List[PrimitiveDict] = [
            dict(zip(catalog_table.column_names, map(_coerce_decimal, row)))
            for row in catalog_table
        ]

        catalog = Catalog(catalog_data)
        results = CatalogResults(
            nodes=catalog.make_unique_id_map(manifest),
            generated_at=datetime.utcnow(),
            _compile_results=compile_results,
        )

        path = os.path.join(self.config.target_path, CATALOG_FILENAME)
        results.write(path)

        dbt.ui.printer.print_timestamped_line(
            'Catalog written to {}'.format(os.path.abspath(path))
        )
        return results

    def interpret_results(self, results):
        compile_results = results._compile_results
        if compile_results is None:
            return True

        return super().interpret_results(compile_results)
