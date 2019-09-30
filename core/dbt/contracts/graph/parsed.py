from dataclasses import dataclass, field
from typing import (
    Optional, Union, List, Dict, Any, Type, Tuple, NewType, MutableMapping
)

from hologram import JsonSchemaMixin
from hologram.helpers import (
    StrEnum, register_pattern
)

import dbt.clients.jinja
import dbt.flags
from dbt.contracts.graph.unparsed import (
    UnparsedNode, UnparsedMacro, UnparsedDocumentationFile, Quoting,
    UnparsedBaseNode, FreshnessThreshold, ExternalTable,
    AdditionalPropertiesAllowed
)
from dbt.contracts.util import Replaceable
from dbt.logger import GLOBAL_LOGGER as logger  # noqa
from dbt.node_types import NodeType


class SnapshotStrategy(StrEnum):
    Timestamp = 'timestamp'
    Check = 'check'


class All(StrEnum):
    All = 'all'


@dataclass
class Hook(JsonSchemaMixin, Replaceable):
    sql: str
    transaction: bool = True
    index: Optional[int] = None


def insensitive_patterns(*patterns: str):
    lowercased = []
    for pattern in patterns:
        lowercased.append(
            ''.join('[{}{}]'.format(s.upper(), s.lower()) for s in pattern)
        )
    return '^({})$'.format('|'.join(lowercased))


Severity = NewType('Severity', str)
register_pattern(Severity, insensitive_patterns('warn', 'error'))


@dataclass
class NodeConfig(
    AdditionalPropertiesAllowed, Replaceable, MutableMapping[str, Any]
):
    enabled: bool = True
    materialized: str = 'view'
    persist_docs: Dict[str, Any] = field(default_factory=dict)
    post_hook: List[Hook] = field(default_factory=list)
    pre_hook: List[Hook] = field(default_factory=list)
    vars: Dict[str, Any] = field(default_factory=dict)
    quoting: Dict[str, Any] = field(default_factory=dict)
    column_types: Dict[str, Any] = field(default_factory=dict)
    tags: Union[List[str], str] = field(default_factory=list)

    @classmethod
    def field_mapping(cls):
        return {'post_hook': 'post-hook', 'pre_hook': 'pre-hook'}

    # Implement MutableMapping so this config will behave as some macros expect
    # during parsing (notably, syntax like `{{ node.config['schema'] }}`)

    def __getitem__(self, key):
        """Handle parse-time use of `config` as a dictionary, making the extra
        values available during parsing.
        """
        if hasattr(self, key):
            return getattr(self, key)
        else:
            return self._extra[key]

    def __setitem__(self, key, value):
        if hasattr(self, key):
            setattr(self, key, value)
        else:
            self._extra[key] = value

    def __delitem__(self, key):
        if hasattr(self, key):
            msg = (
                'Error, tried to delete config key "{}": Cannot delete '
                'built-in keys'
            ).format(key)
            raise dbt.exceptions.CompilationException(msg)
        else:
            del self._extra[key]

    def __iter__(self):
        for fld in self._get_fields():
            yield fld.name

        for key in self._extra:
            yield key

    def __len__(self):
        return len(self._get_fields()) + len(self._extra)


@dataclass
class ColumnInfo(JsonSchemaMixin, Replaceable):
    name: str
    description: str = ''
    data_type: Optional[str] = None


# Docrefs are not quite like regular references, as they indicate what they
# apply to as well as what they are referring to (so the doc package + doc
# name, but also the column name if relevant). This is because column
# descriptions are rendered separately from their models.
@dataclass
class Docref(JsonSchemaMixin, Replaceable):
    documentation_name: str
    documentation_package: str
    column_name: Optional[str] = None


@dataclass
class HasFqn(JsonSchemaMixin, Replaceable):
    fqn: List[str]


@dataclass
class HasUniqueID(JsonSchemaMixin, Replaceable):
    unique_id: str


@dataclass
class DependsOn(JsonSchemaMixin, Replaceable):
    nodes: List[str] = field(default_factory=list)
    macros: List[str] = field(default_factory=list)


@dataclass
class HasRelationMetadata(JsonSchemaMixin, Replaceable):
    database: str
    schema: str


class ParsedNodeMixins:
    @property
    def is_refable(self):
        return self.resource_type in NodeType.refable()

    @property
    def is_ephemeral(self):
        return self.config.materialized == 'ephemeral'

    @property
    def is_ephemeral_model(self):
        return self.is_refable and self.is_ephemeral

    @property
    def depends_on_nodes(self):
        return self.depends_on.nodes

    def patch(self, patch):
        """Given a ParsedNodePatch, add the new information to the node."""
        # explicitly pick out the parts to update so we don't inadvertently
        # step on the model name or anything
        self.patch_path = patch.original_file_path
        self.description = patch.description
        self.columns = patch.columns
        self.docrefs = patch.docrefs
        if dbt.flags.STRICT_MODE:
            self.to_dict(validate=True)

    def get_materialization(self):
        return self.config.materialized

    def local_vars(self):
        return self.config.vars


@dataclass
class ParsedNodeMandatory(
    UnparsedNode,
    HasUniqueID,
    HasFqn,
    HasRelationMetadata,
    Replaceable
):
    alias: str

    @property
    def identifier(self):
        return self.alias


@dataclass
class ParsedNodeDefaults(ParsedNodeMandatory):
    config: NodeConfig = field(default_factory=NodeConfig)
    tags: List[str] = field(default_factory=list)
    refs: List[List[str]] = field(default_factory=list)
    sources: List[List[Any]] = field(default_factory=list)
    depends_on: DependsOn = field(default_factory=DependsOn)
    docrefs: List[Docref] = field(default_factory=list)
    description: str = field(default='')
    columns: Dict[str, ColumnInfo] = field(default_factory=dict)
    patch_path: Optional[str] = None
    build_path: Optional[str] = None


@dataclass
class ParsedNode(ParsedNodeDefaults, ParsedNodeMixins):
    pass


@dataclass
class ParsedAnalysisNode(ParsedNode):
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Analysis]})


@dataclass
class ParsedHookNode(ParsedNode):
    resource_type: NodeType = field(
        metadata={'restrict': [NodeType.Operation]}
    )
    index: Optional[int] = None


@dataclass
class ParsedModelNode(ParsedNode):
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Model]})


@dataclass
class ParsedRPCNode(ParsedNode):
    resource_type: NodeType = field(metadata={'restrict': [NodeType.RPCCall]})


@dataclass
class ParsedSeedNode(ParsedNode):
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Seed]})
    seed_file_path: str = ''

    def __post_init__(self):
        if self.seed_file_path == '':
            raise dbt.exceptions.InternalException(
                'Seeds should always have a seed_file_path'
            )

    @property
    def empty(self):
        """ Seeds are never empty"""
        return False


@dataclass
class TestConfig(NodeConfig):
    severity: Severity = 'error'


@dataclass
class ParsedTestNode(ParsedNode):
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Test]})
    column_name: Optional[str] = None
    config: TestConfig = field(default_factory=TestConfig)


@dataclass(init=False)
class _SnapshotConfig(NodeConfig):
    unique_key: str
    target_schema: str
    target_database: Optional[str] = None

    def __init__(
        self,
        unique_key: str,
        target_schema: str,
        target_database: Optional[str] = None,
        **kwargs
    ) -> None:
        self.target_database = target_database
        self.target_schema = target_schema
        self.unique_key = unique_key
        super().__init__(**kwargs)


@dataclass(init=False)
class GenericSnapshotConfig(_SnapshotConfig):
    strategy: SnapshotStrategy

    def __init__(self, strategy: str, **kwargs) -> None:
        self.strategy = strategy
        super().__init__(**kwargs)


@dataclass(init=False)
class TimestampSnapshotConfig(_SnapshotConfig):
    strategy: SnapshotStrategy = field(metadata={
        'restrict': [SnapshotStrategy.Timestamp]
    })
    updated_at: str

    def __init__(
        self, strategy: SnapshotStrategy, updated_at: str, **kwargs
    ) -> None:
        self.strategy = strategy
        self.updated_at = updated_at
        super().__init__(**kwargs)


@dataclass(init=False)
class CheckSnapshotConfig(_SnapshotConfig):
    strategy: SnapshotStrategy = field(metadata={
        'restrict': [SnapshotStrategy.Check]
    })
    # TODO: is there a way to get this to accept tuples of strings? Adding
    # `Tuple[str, ...]` to the list of types results in this:
    # ['email'] is valid under each of {'type': 'array', 'items':
    # {'type': 'string'}}, {'type': 'array', 'items': {'type': 'string'}}
    # but without it, parsing gets upset about values like `('email',)`
    # maybe hologram itself should support this behavior? It's not like tuples
    # are meaningful in json
    check_cols: Union[All, List[str]]

    def __init__(
        self, strategy: SnapshotStrategy, check_cols: Union[All, List[str]],
        **kwargs
    ) -> None:
        self.strategy = strategy
        self.check_cols = check_cols
        super().__init__(**kwargs)


@dataclass
class IntermediateSnapshotNode(ParsedNode):
    # at an intermediate stage in parsing, where we've built something better
    # than an unparsed node for rendering in parse mode, it's pretty possible
    # that we won't have critical snapshot-related information that is only
    # defined in config blocks. To fix that, we have an intermediate type that
    # uses a regular node config, which the snapshot parser will then convert
    # into a full ParsedSnapshotNode after rendering.
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Snapshot]})


def _create_if_else_chain(
    key: str,
    criteria: List[Tuple[str, Type[JsonSchemaMixin]]],
    default: Type[JsonSchemaMixin]
) -> dict:
    """Mutate a given schema key that contains a 'oneOf' to instead be an
    'if-then-else' chain. This results is much better/more consistent errors
    from jsonschema.
    """
    result = schema = {}
    criteria = criteria[:]
    while criteria:
        if_clause, then_clause = criteria.pop()
        schema['if'] = {'properties': {
            key: {'enum': [if_clause]}
        }}
        schema['then'] = then_clause.json_schema()
        schema['else'] = {}
        schema = schema['else']
    schema.update(default.json_schema())
    return result


@dataclass
class ParsedSnapshotNode(ParsedNode):
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Snapshot]})
    config: Union[
        CheckSnapshotConfig,
        TimestampSnapshotConfig,
        GenericSnapshotConfig,
    ]

    @classmethod
    def json_schema(cls, embeddable=False):
        schema = super().json_schema(embeddable)

        # mess with config
        configs = [
            (str(SnapshotStrategy.Check), CheckSnapshotConfig),
            (str(SnapshotStrategy.Timestamp), TimestampSnapshotConfig),
        ]

        if embeddable:
            dest = schema[cls.__name__]['properties']
        else:
            dest = schema['properties']
        dest['config'] = _create_if_else_chain(
            'strategy', configs, GenericSnapshotConfig
        )
        return schema


# The parsed node update is only the 'patch', not the test. The test became a
# regular parsed node. Note that description and columns must be present, but
# may be empty.
@dataclass
class ParsedNodePatch(JsonSchemaMixin, Replaceable):
    name: str
    description: str
    original_file_path: str
    columns: Dict[str, ColumnInfo]
    docrefs: List[Docref]


@dataclass
class MacroDependsOn(JsonSchemaMixin, Replaceable):
    macros: List[str] = field(default_factory=list)


@dataclass
class ParsedMacro(UnparsedMacro, HasUniqueID):
    name: str
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Macro]})
    # TODO: can macros even have tags?
    tags: List[str] = field(default_factory=list)
    # TODO: is this ever populated?
    depends_on: MacroDependsOn = field(default_factory=MacroDependsOn)

    def local_vars(self):
        return {}

    @property
    def generator(self):
        """
        Returns a function that can be called to render the macro results.
        """
        return dbt.clients.jinja.macro_generator(self)


@dataclass
class ParsedDocumentation(UnparsedDocumentationFile, HasUniqueID):
    name: str
    block_contents: str


@dataclass
class ParsedSourceDefinition(
        UnparsedBaseNode,
        HasUniqueID,
        HasRelationMetadata,
        HasFqn):
    name: str
    source_name: str
    source_description: str
    loader: str
    identifier: str
    resource_type: NodeType = field(metadata={'restrict': [NodeType.Source]})
    quoting: Quoting = field(default_factory=Quoting)
    loaded_at_field: Optional[str] = None
    freshness: Optional[FreshnessThreshold] = None
    external: Optional[ExternalTable] = None
    docrefs: List[Docref] = field(default_factory=list)
    description: str = ''
    columns: Dict[str, ColumnInfo] = field(default_factory=dict)

    @property
    def is_ephemeral_model(self):
        return False

    @property
    def depends_on_nodes(self):
        return []

    @property
    def refs(self):
        return []

    @property
    def sources(self):
        return []

    @property
    def tags(self):
        return []

    @property
    def has_freshness(self):
        return bool(self.freshness) and self.loaded_at_field is not None


PARSED_TYPES = {
    NodeType.Analysis: ParsedAnalysisNode,
    NodeType.Documentation: ParsedDocumentation,
    NodeType.Macro: ParsedMacro,
    NodeType.Model: ParsedModelNode,
    NodeType.Operation: ParsedHookNode,
    NodeType.RPCCall: ParsedRPCNode,
    NodeType.Seed: ParsedSeedNode,
    NodeType.Snapshot: ParsedSnapshotNode,
    NodeType.Source: ParsedSourceDefinition,
    NodeType.Test: ParsedTestNode,
}
