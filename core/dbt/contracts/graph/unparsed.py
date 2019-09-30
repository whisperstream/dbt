from dbt.node_types import NodeType
from dbt.contracts.util import Replaceable, Mergeable
from dbt.exceptions import CompilationException

from hologram import JsonSchemaMixin
from hologram.helpers import StrEnum, ExtensibleJsonSchemaMixin

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Optional, List, Union, Dict, Any


@dataclass
class UnparsedBaseNode(JsonSchemaMixin, Replaceable):
    package_name: str
    root_path: str
    path: str
    original_file_path: str


@dataclass
class HasSQL:
    raw_sql: str

    @property
    def empty(self):
        return not self.raw_sql.strip()


@dataclass
class UnparsedMacro(UnparsedBaseNode, HasSQL):
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Macro]})


@dataclass
class UnparsedNode(UnparsedBaseNode, HasSQL):
    name: str
    resource_type: NodeType = field(metadata={'restrict': [
        NodeType.Model,
        NodeType.Analysis,
        NodeType.Test,
        NodeType.Snapshot,
        NodeType.Operation,
        NodeType.Seed,
        NodeType.RPCCall,
    ]})


@dataclass
class UnparsedRunHook(UnparsedNode):
    resource_type: NodeType = field(
        metadata={'restrict': [NodeType.Operation]}
    )
    index: Optional[int] = None


@dataclass
class NamedTested(JsonSchemaMixin, Replaceable):
    name: str
    description: str = ''
    data_type: Optional[str] = None
    tests: Optional[List[Union[Dict[str, Any], str]]] = None

    def __post_init__(self):
        if self.tests is None:
            self.tests = []


@dataclass
class ColumnDescription(JsonSchemaMixin, Replaceable):
    columns: Optional[List[NamedTested]] = field(default_factory=list)

    def __post_init__(self):
        if self.columns is None:
            self.columns = []


@dataclass
class NodeDescription(NamedTested):
    pass


@dataclass
class UnparsedNodeUpdate(ColumnDescription, NodeDescription):
    def __post_init__(self):
        NodeDescription.__post_init__(self)
        ColumnDescription.__post_init__(self)


class TimePeriod(StrEnum):
    minute = 'minute'
    hour = 'hour'
    day = 'day'

    def plural(self) -> str:
        return str(self) + 's'


@dataclass
class Time(JsonSchemaMixin, Replaceable):
    count: int
    period: TimePeriod

    def exceeded(self, actual_age: float) -> bool:
        kwargs = {self.period.plural(): self.count}
        difference = timedelta(**kwargs).total_seconds()
        return actual_age > difference


class FreshnessStatus(StrEnum):
    Pass = 'pass'
    Warn = 'warn'
    Error = 'error'


@dataclass
class FreshnessThreshold(JsonSchemaMixin, Mergeable):
    warn_after: Optional[Time] = None
    error_after: Optional[Time] = None
    filter: Optional[str] = None

    def status(self, age: float) -> FreshnessStatus:
        if self.error_after and self.error_after.exceeded(age):
            return FreshnessStatus.Error
        elif self.warn_after and self.warn_after.exceeded(age):
            return FreshnessStatus.Warn
        else:
            return FreshnessStatus.Pass

    def __bool__(self):
        return self.warn_after is not None or self.error_after is not None


@dataclass
class AdditionalPropertiesAllowed(ExtensibleJsonSchemaMixin):
    _extra: Dict[str, Any] = field(default_factory=dict)

    @property
    def extra(self):
        return self._extra

    @classmethod
    def from_dict(cls, data, validate=True):
        self = super().from_dict(data=data, validate=validate)
        keys = self.to_dict(validate=False, omit_none=False)
        for key, value in data.items():
            if key not in keys:
                self._extra[key] = value
        return self

    def to_dict(self, omit_none=True, validate=False):
        data = super().to_dict(omit_none=omit_none, validate=validate)
        data.update(self._extra)
        return data

    def replace(self, **kwargs):
        dct = self.to_dict(omit_none=False, validate=False)
        dct.update(kwargs)
        return self.from_dict(dct)


@dataclass
class ExternalPartition(AdditionalPropertiesAllowed, Replaceable):
    name: str = ''
    description: str = ''
    data_type: str = ''

    def __post_init__(self):
        if self.name == '' or self.data_type == '':
            raise CompilationException(
                'External partition columns must have names and data types'
            )


@dataclass
class ExternalTable(AdditionalPropertiesAllowed, Mergeable):
    location: Optional[str] = None
    file_format: Optional[str] = None
    row_format: Optional[str] = None
    tbl_properties: Optional[str] = None
    partitions: Optional[List[ExternalPartition]] = None

    def __bool__(self):
        return self.location is not None


@dataclass
class Quoting(JsonSchemaMixin, Mergeable):
    database: Optional[bool] = None
    schema: Optional[bool] = None
    identifier: Optional[bool] = None


@dataclass
class UnparsedSourceTableDefinition(ColumnDescription, NodeDescription):
    loaded_at_field: Optional[str] = None
    identifier: Optional[str] = None
    quoting: Quoting = field(default_factory=Quoting)
    freshness: Optional[FreshnessThreshold] = field(
        default_factory=FreshnessThreshold
    )
    external: Optional[ExternalTable] = field(
        default_factory=ExternalTable
    )

    def __post_init__(self):
        NodeDescription.__post_init__(self)
        ColumnDescription.__post_init__(self)


@dataclass
class UnparsedSourceDefinition(JsonSchemaMixin, Replaceable):
    name: str
    description: str = ''
    database: Optional[str] = None
    schema: Optional[str] = None
    loader: str = ''
    quoting: Quoting = field(default_factory=Quoting)
    freshness: Optional[FreshnessThreshold] = field(
        default_factory=FreshnessThreshold
    )
    loaded_at_field: Optional[str] = None
    tables: List[UnparsedSourceTableDefinition] = field(default_factory=list)


@dataclass
class UnparsedDocumentationFile(JsonSchemaMixin, Replaceable):
    package_name: str
    root_path: str
    path: str
    original_file_path: str
    file_contents: str

    @property
    def resource_type(self):
        return NodeType.Documentation
