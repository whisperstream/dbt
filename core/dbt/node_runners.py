import threading
import time
import traceback
from typing import List, Dict, Any

from dbt import deprecations
from dbt.adapters.base import BaseRelation
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.exceptions import (
    NotImplementedException, CompilationException, RuntimeException,
    InternalException, missing_materialization
)
from dbt.node_types import NodeType
from dbt.contracts.results import (
    RunModelResult, collect_timing_info, SourceFreshnessResult, PartialResult,
)
from dbt.compilation import compile_node

import dbt.context.runtime
import dbt.exceptions
import dbt.utils
import dbt.tracking
import dbt.ui.printer
import dbt.flags


INTERNAL_ERROR_STRING = """This is an error in dbt. Please try again. If \
the error persists, open an issue at https://github.com/fishtown-analytics/dbt
""".strip()


def track_model_run(index, num_nodes, run_model_result):
    invocation_id = dbt.tracking.active_user.invocation_id
    dbt.tracking.track_model_run({
        "invocation_id": invocation_id,
        "index": index,
        "total": num_nodes,
        "execution_time": run_model_result.execution_time,
        "run_status": run_model_result.status,
        "run_skipped": run_model_result.skip,
        "run_error": None,
        "model_materialization": dbt.utils.get_materialization(
            run_model_result.node
        ),
        "model_id": dbt.utils.get_hash(run_model_result.node),
        "hashed_contents": dbt.utils.get_hashed_contents(
            run_model_result.node
        ),
        "timing": run_model_result.timing,
    })


class ExecutionContext:
    """During execution and error handling, dbt makes use of mutable state:
    timing information and the newest (compiled vs executed) form of the node.
    """
    def __init__(self, node):
        self.timing = []
        self.node = node


class BaseRunner:
    def __init__(self, config, adapter, node, node_index, num_nodes):
        self.config = config
        self.adapter = adapter
        self.node = node
        self.node_index = node_index
        self.num_nodes = num_nodes

        self.skip = False
        self.skip_cause = None

    def run_with_hooks(self, manifest):
        if self.skip:
            return self.on_skip()

        # no before/after printing for ephemeral mdoels
        if not self.node.is_ephemeral_model:
            self.before_execute()

        result = self.safe_run(manifest)

        if not self.node.is_ephemeral_model:
            self.after_execute(result)

        return result

    def _build_run_result(self, node, start_time, error, status, timing_info,
                          skip=False, fail=None, warn=None, agate_table=None):
        execution_time = time.time() - start_time
        thread_id = threading.current_thread().name
        return RunModelResult(
            node=node,
            error=error,
            skip=skip,
            status=status,
            fail=fail,
            warn=warn,
            execution_time=execution_time,
            thread_id=thread_id,
            timing=timing_info,
            agate_table=agate_table,
        )

    def error_result(self, node, error, start_time, timing_info):
        return self._build_run_result(
            node=node,
            start_time=start_time,
            error=error,
            status='ERROR',
            timing_info=timing_info
        )

    def ephemeral_result(self, node, start_time, timing_info):
        return self._build_run_result(
            node=node,
            start_time=start_time,
            error=None,
            status=None,
            timing_info=timing_info
        )

    def from_run_result(self, result, start_time, timing_info):
        return self._build_run_result(
            node=result.node,
            start_time=start_time,
            error=result.error,
            skip=result.skip,
            status=result.status,
            fail=result.fail,
            warn=result.warn,
            timing_info=timing_info,
            agate_table=result.agate_table,
        )

    def compile_and_execute(self, manifest, ctx):
        result = None
        self.adapter.acquire_connection(self.node.name)
        with collect_timing_info('compile') as timing_info:
            # if we fail here, we still have a compiled node to return
            # this has the benefit of showing a build path for the errant
            # model
            ctx.node = self.compile(manifest)
        ctx.timing.append(timing_info)

        # for ephemeral nodes, we only want to compile, not run
        if not ctx.node.is_ephemeral_model:
            with collect_timing_info('execute') as timing_info:
                result = self.run(ctx.node, manifest)
                ctx.node = result.node

            ctx.timing.append(timing_info)

        return result

    def _handle_catchable_exception(self, e, ctx):
        if e.node is None:
            e.node = ctx.node

        logger.debug(str(e), exc_info=True)
        return str(e)

    def _handle_internal_exception(self, e, ctx):
        build_path = self.node.build_path
        prefix = 'Internal error executing {}'.format(build_path)

        error = "{prefix}\n{error}\n\n{note}".format(
            prefix=dbt.ui.printer.red(prefix),
            error=str(e).strip(),
            note=INTERNAL_ERROR_STRING
        )
        logger.debug(error, exc_info=True)
        return str(e)

    def _handle_generic_exception(self, e, ctx):
        node_description = self.node.build_path
        if node_description is None:
            node_description = self.node.unique_id
        prefix = "Unhandled error while executing {}".format(node_description)
        error = "{prefix}\n{error}".format(
            prefix=dbt.ui.printer.red(prefix),
            error=str(e).strip()
        )

        logger.error(error)
        logger.debug('', exc_info=True)
        return str(e)

    def handle_exception(self, e, ctx):
        catchable_errors = (CompilationException, RuntimeException)
        if isinstance(e, catchable_errors):
            error = self._handle_catchable_exception(e, ctx)
        elif isinstance(e, InternalException):
            error = self._handle_internal_exception(e, ctx)
        else:
            error = self._handle_generic_exception(e, ctx)
        return error

    def safe_run(self, manifest):
        started = time.time()
        ctx = ExecutionContext(self.node)
        error = None
        result = None

        try:
            result = self.compile_and_execute(manifest, ctx)
        except Exception as e:
            error = self.handle_exception(e, ctx)
        finally:
            exc_str = self._safe_release_connection()

            # if releasing failed and the result doesn't have an error yet, set
            # an error
            if exc_str is not None and result.error is None:
                error = exc_str

        if error is not None:
            # we could include compile time for runtime errors here
            result = self.error_result(ctx.node, error, started, [])
        elif result is not None:
            result = self.from_run_result(result, started, ctx.timing)
        else:
            result = self.ephemeral_result(ctx.node, started, ctx.timing)
        return result

    def _safe_release_connection(self):
        """Try to release a connection. If an exception is hit, log and return
        the error string.
        """
        try:
            self.adapter.release_connection()
        except Exception as exc:
            logger.debug(
                'Error releasing connection for node {}: {!s}\n{}'
                .format(self.node.name, exc, traceback.format_exc())
            )
            return str(exc)

        return None

    def before_execute(self):
        raise NotImplementedException()

    def execute(self, compiled_node, manifest):
        raise NotImplementedException()

    def run(self, compiled_node, manifest):
        return self.execute(compiled_node, manifest)

    def after_execute(self, result):
        raise NotImplementedException()

    def _skip_caused_by_ephemeral_failure(self):
        if self.skip_cause is None or self.skip_cause.node is None:
            return False
        return self.skip_cause.node.is_ephemeral_model

    def on_skip(self):
        schema_name = self.node.schema
        node_name = self.node.name

        error = None
        if not self.node.is_ephemeral_model:
            # if this model was skipped due to an upstream ephemeral model
            # failure, print a special 'error skip' message.
            if self._skip_caused_by_ephemeral_failure():
                dbt.ui.printer.print_skip_caused_by_error(
                    self.node,
                    schema_name,
                    node_name,
                    self.node_index,
                    self.num_nodes,
                    self.skip_cause
                )
                # set an error so dbt will exit with an error code
                error = (
                    'Compilation Error in {}, caused by compilation error '
                    'in referenced ephemeral model {}'
                    .format(self.node.unique_id,
                            self.skip_cause.node.unique_id)
                )
            else:
                dbt.ui.printer.print_skip_line(
                    self.node,
                    schema_name,
                    node_name,
                    self.node_index,
                    self.num_nodes
                )

        node_result = RunModelResult(self.node, skip=True, error=error)
        return node_result

    def do_skip(self, cause=None):
        self.skip = True
        self.skip_cause = cause


class CompileRunner(BaseRunner):
    def before_execute(self):
        pass

    def after_execute(self, result):
        pass

    def execute(self, compiled_node, manifest):
        return RunModelResult(compiled_node)

    def compile(self, manifest):
        return compile_node(self.adapter, self.config, self.node, manifest, {})


# make sure that we got an ok result back from a materialization
def _validate_materialization_relations_dict(
    inp: Dict[Any, Any], model
) -> List[BaseRelation]:
    try:
        relations_value = inp['relations']
    except KeyError:
        msg = (
            'Invalid return value from materialization, "relations" '
            'not found, got keys: {}'.format(list(inp))
        )
        raise CompilationException(msg, node=model) from None

    if not isinstance(relations_value, list):
        msg = (
            'Invalid return value from materialization, "relations" '
            'not a list, got: {}'.format(relations_value)
        )
        raise CompilationException(msg, node=model) from None

    relations: List[BaseRelation] = []
    for relation in relations_value:
        if not isinstance(relation, BaseRelation):
            msg = (
                'Invalid return value from materialization, '
                '"relations" contains non-Relation: {}'
                .format(relation)
            )
            raise CompilationException(msg, node=model)

        assert isinstance(relation, BaseRelation)
        relations.append(relation)
    return relations


class ModelRunner(CompileRunner):
    def get_node_representation(self):
        if self.config.credentials.database == self.node.database:
            template = "{0.schema}.{0.alias}"
        else:
            template = "{0.database}.{0.schema}.{0.alias}"

        return template.format(self.node)

    def describe_node(self):
        return "{} model {}".format(self.node.get_materialization(),
                                    self.get_node_representation())

    def print_start_line(self):
        description = self.describe_node()
        dbt.ui.printer.print_start_line(description, self.node_index,
                                        self.num_nodes)

    def print_result_line(self, result):
        description = self.describe_node()
        dbt.ui.printer.print_model_result_line(result,
                                               description,
                                               self.node_index,
                                               self.num_nodes)

    def before_execute(self):
        self.print_start_line()

    def after_execute(self, result):
        track_model_run(self.node_index, self.num_nodes, result)
        self.print_result_line(result)

    def _build_run_model_result(self, model, context):
        result = context['load_result']('main')
        return RunModelResult(model, status=result.status)

    def _materialization_relations(
        self, result: Any, model
    ) -> List[BaseRelation]:
        if isinstance(result, str):
            deprecations.warn('materialization-return',
                              materialization=model.get_materialization())
            return [
                self.adapter.Relation.create_from(self.config, model)
            ]

        if isinstance(result, dict):
            return _validate_materialization_relations_dict(result, model)

        msg = (
            'Invalid return value from materialization, expected a dict '
            'with key "relations", got: {}'.format(str(result))
        )
        raise CompilationException(msg, node=model)

    def execute(self, model, manifest):
        context = dbt.context.runtime.generate(
            model, self.config, manifest)

        materialization_macro = manifest.get_materialization_macro(
            model.get_materialization(),
            self.adapter.type())

        if materialization_macro is None:
            missing_materialization(model, self.adapter.type())

        if 'config' not in context:
            raise InternalException(
                'Invalid materialization context generated, missing config: {}'
                .format(context)
            )
        context_config = context['config']

        hook_ctx = self.adapter.pre_model_hook(context_config)
        try:
            result = materialization_macro.generator(context)()
        finally:
            self.adapter.post_model_hook(context_config, hook_ctx)

        for relation in self._materialization_relations(result, model):
            self.adapter.cache_added(relation.incorporate(dbt_created=True))

        return self._build_run_model_result(model, context)


class FreshnessRunner(BaseRunner):
    def on_skip(self):
        raise RuntimeException(
            'Freshness: nodes cannot be skipped!'
        )

    def before_execute(self):
        description = 'freshness of {0.source_name}.{0.name}'.format(self.node)
        dbt.ui.printer.print_start_line(description, self.node_index,
                                        self.num_nodes)

    def after_execute(self, result):
        dbt.ui.printer.print_freshness_result_line(result,
                                                   self.node_index,
                                                   self.num_nodes)

    def _build_run_result(self, node, start_time, error, status, timing_info,
                          skip=False, failed=None):
        execution_time = time.time() - start_time
        thread_id = threading.current_thread().name
        if status is not None:
            status = status.lower()
        return PartialResult(
            node=node,
            status=status,
            error=error,
            execution_time=execution_time,
            thread_id=thread_id,
            timing=timing_info,
        )

    def from_run_result(self, result, start_time, timing_info):
        result.execution_time = (time.time() - start_time)
        result.timing.extend(timing_info)
        return result

    def execute(self, compiled_node, manifest):
        # we should only be here if we compiled_node.has_freshness, and
        # therefore loaded_at_field should be a str. If this invariant is
        # broken, raise!
        if compiled_node.loaded_at_field is None:
            raise InternalException(
                'Got to execute for source freshness of a source that has no '
                'loaded_at_field!'
            )

        relation = self.adapter.Relation.create_from_source(compiled_node)
        # given a Source, calculate its fresnhess.
        with self.adapter.connection_named(compiled_node.unique_id):
            self.adapter.clear_transaction()
            freshness = self.adapter.calculate_freshness(
                relation,
                compiled_node.loaded_at_field,
                compiled_node.freshness.filter,
                manifest=manifest
            )

        status = compiled_node.freshness.status(freshness['age'])

        return SourceFreshnessResult(
            node=compiled_node,
            status=status,
            thread_id=threading.current_thread().name,
            **freshness
        )

    def compile(self, manifest):
        if self.node.resource_type != NodeType.Source:
            # should be unreachable...
            raise RuntimeException('fresnhess runner: got a non-Source')
        # we don't do anything interesting when we compile a source node
        return self.node


class TestRunner(CompileRunner):
    def describe_node(self):
        node_name = self.node.name
        return "test {}".format(node_name)

    def print_result_line(self, result):
        schema_name = self.node.schema
        dbt.ui.printer.print_test_result_line(result,
                                              schema_name,
                                              self.node_index,
                                              self.num_nodes)

    def print_start_line(self):
        description = self.describe_node()
        dbt.ui.printer.print_start_line(description, self.node_index,
                                        self.num_nodes)

    def execute_test(self, test):
        res, table = self.adapter.execute(
            test.wrapped_sql,
            auto_begin=True,
            fetch=True)

        num_rows = len(table.rows)
        if num_rows > 1:
            num_cols = len(table.columns)
            raise RuntimeError(
                "Bad test {name}: Returned {rows} rows and {cols} cols"
                .format(name=test.name, rows=num_rows, cols=num_cols))

        return table[0][0]

    def before_execute(self):
        self.print_start_line()

    def execute(self, test, manifest):
        failed_rows = self.execute_test(test)
        severity = test.config.severity.upper()

        if failed_rows == 0:
            return RunModelResult(test, status=failed_rows)
        elif severity == 'ERROR' or dbt.flags.WARN_ERROR:
            return RunModelResult(test, status=failed_rows, fail=True)
        else:
            return RunModelResult(test, status=failed_rows, warn=True)

    def after_execute(self, result):
        self.print_result_line(result)


class SnapshotRunner(ModelRunner):
    def describe_node(self):
        return "snapshot {}".format(self.get_node_representation())

    def print_result_line(self, result):
        dbt.ui.printer.print_snapshot_result_line(
            result,
            self.get_node_representation(),
            self.node_index,
            self.num_nodes)


class SeedRunner(ModelRunner):
    def describe_node(self):
        return "seed file {}".format(self.get_node_representation())

    def before_execute(self):
        description = self.describe_node()
        dbt.ui.printer.print_start_line(description, self.node_index,
                                        self.num_nodes)

    def _build_run_model_result(self, model, context):
        result = super()._build_run_model_result(model, context)
        agate_result = context['load_result']('agate_table')
        result.agate_table = agate_result.table
        return result

    def compile(self, manifest):
        return self.node

    def print_result_line(self, result):
        schema_name = self.node.schema
        dbt.ui.printer.print_seed_result_line(result,
                                              schema_name,
                                              self.node_index,
                                              self.num_nodes)
