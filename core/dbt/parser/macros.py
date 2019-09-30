from typing import Iterable

import jinja2

from dbt.clients import jinja
from dbt.contracts.graph.unparsed import UnparsedMacro
from dbt.contracts.graph.parsed import ParsedMacro
from dbt.exceptions import CompilationException
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.node_types import NodeType
from dbt.parser.base import BaseParser
from dbt.parser.search import FileBlock, FilesystemSearcher
from dbt.utils import MACRO_PREFIX


class MacroParser(BaseParser[ParsedMacro]):
    def get_paths(self):
        return FilesystemSearcher(
            project=self.project,
            relative_dirs=self.project.macro_paths,
            extension='.sql',
        )

    @property
    def resource_type(self) -> NodeType:
        return NodeType.Macro

    @classmethod
    def get_compiled_path(cls, block: FileBlock):
        return block.path.relative_path

    def parse_macro(self, base_node: UnparsedMacro, name: str) -> ParsedMacro:
        unique_id = self.generate_unique_id(name)

        return ParsedMacro(
            path=base_node.path,
            original_file_path=base_node.original_file_path,
            package_name=base_node.package_name,
            raw_sql=base_node.raw_sql,
            root_path=base_node.root_path,
            resource_type=base_node.resource_type,
            name=name,
            unique_id=unique_id,
        )

    def parse_unparsed_macros(
        self, base_node: UnparsedMacro
    ) -> Iterable[ParsedMacro]:
        try:
            ast = jinja.parse(base_node.raw_sql)
        except CompilationException as e:
            e.node = base_node
            raise e

        for macro_node in ast.find_all(jinja2.nodes.Macro):
            macro_name = macro_node.name

            if not macro_name.startswith(MACRO_PREFIX):
                continue

            name = macro_name.replace(MACRO_PREFIX, '')
            node = self.parse_macro(base_node, name)
            yield node

    def parse_file(self, block: FileBlock):
        # mark the file as seen, even if there are no macros in it
        self.results.get_file(block.file)
        source_file = block.file

        original_file_path = source_file.path.original_file_path

        logger.debug("Parsing {}".format(original_file_path))

        # this is really only used for error messages
        base_node = UnparsedMacro(
            path=original_file_path,
            original_file_path=original_file_path,
            package_name=self.project.project_name,
            raw_sql=source_file.contents,
            root_path=self.project.project_root,
            resource_type=NodeType.Macro,
        )

        for node in self.parse_unparsed_macros(base_node):
            self.results.add_macro(block.file, node)
