import agate
import json
import os
from typing import Union, Callable

from dbt.adapters.factory import get_adapter
from dbt.node_types import NodeType
from dbt.include.global_project import PACKAGES
from dbt.include.global_project import PROJECT_NAME as GLOBAL_PROJECT_NAME

import dbt.clients.jinja
import dbt.clients.agate_helper
from dbt.contracts.graph.compiled import CompiledSeedNode
from dbt.contracts.graph.parsed import ParsedSeedNode
import dbt.exceptions
import dbt.flags
import dbt.tracking
import dbt.writer
import dbt.utils

from dbt.logger import GLOBAL_LOGGER as logger  # noqa


# These modules are added to the context. Consider alternative
# approaches which will extend well to potentially many modules
import pytz
import datetime
import re


class RelationProxy:
    def __init__(self, adapter):
        self.quoting_config = adapter.config.quoting
        self.relation_type = adapter.Relation

    def __getattr__(self, key):
        return getattr(self.relation_type, key)

    def create_from_source(self, *args, **kwargs):
        # bypass our create when creating from source so as not to mess up
        # the source quoting
        return self.relation_type.create_from_source(*args, **kwargs)

    def create(self, *args, **kwargs):
        kwargs['quote_policy'] = dbt.utils.merge(
            self.quoting_config,
            kwargs.pop('quote_policy', {})
        )
        return self.relation_type.create(*args, **kwargs)


class BaseDatabaseWrapper:
    """
    Wrapper for runtime database interaction. Applies the runtime quote policy
    via a relation proxy.
    """
    def __init__(self, adapter):
        self.adapter = adapter
        self.Relation = RelationProxy(adapter)

    def __getattr__(self, name):
        raise NotImplementedError('subclasses need to implement this')

    @property
    def config(self):
        return self.adapter.config

    def type(self):
        return self.adapter.type()

    def commit(self):
        return self.adapter.commit_if_has_connection()


class BaseResolver:
    def __init__(self, db_wrapper, model, config, manifest):
        self.db_wrapper = db_wrapper
        self.model = model
        self.config = config
        self.manifest = manifest

    @property
    def current_project(self):
        return self.config.project_name

    @property
    def Relation(self):
        return self.db_wrapper.Relation


def _add_macro_map(context, package_name, macro_map):
    """Update an existing context in-place, adding the given macro map to the
    appropriate package namespace. Adapter packages get inserted into the
    global namespace.
    """
    key = package_name
    if package_name in PACKAGES:
        key = GLOBAL_PROJECT_NAME
    if key not in context:
        context[key] = {}

    context[key].update(macro_map)


def _add_macros(context, model, manifest):
    macros_to_add = {'global': [], 'local': []}

    for unique_id, macro in manifest.macros.items():
        if macro.resource_type != NodeType.Macro:
            continue
        package_name = macro.package_name

        macro_map = {
            macro.name: macro.generator(context)
        }

        # adapter packages are part of the global project space
        _add_macro_map(context, package_name, macro_map)

        if package_name == model.package_name:
            macros_to_add['local'].append(macro_map)
        elif package_name in PACKAGES:
            macros_to_add['global'].append(macro_map)

    # Load global macros before local macros -- local takes precedence
    unprefixed_macros = macros_to_add['global'] + macros_to_add['local']
    for macro_map in unprefixed_macros:
        context.update(macro_map)

    return context


def _add_tracking(context):
    if dbt.tracking.active_user is not None:
        context = dbt.utils.merge(context, {
            "run_started_at": dbt.tracking.active_user.run_started_at,
            "invocation_id": dbt.tracking.active_user.invocation_id,
        })
    else:
        context = dbt.utils.merge(context, {
            "run_started_at": None,
            "invocation_id": None
        })

    return context


def _add_validation(context):
    def validate_any(*args):
        def inner(value):
            for arg in args:
                if isinstance(arg, type) and isinstance(value, arg):
                    return
                elif value == arg:
                    return
            raise dbt.exceptions.ValidationException(
                'Expected value "{}" to be one of {}'
                .format(value, ','.join(map(str, args))))
        return inner

    validation_utils = dbt.utils.AttrDict({
        'any': validate_any,
    })

    return dbt.utils.merge(
        context,
        {'validation': validation_utils})


def env_var(var, default=None):
    if var in os.environ:
        return os.environ[var]
    elif default is not None:
        return default
    else:
        msg = "Env var required but not provided: '{}'".format(var)
        dbt.clients.jinja.undefined_error(msg)


def _store_result(sql_results):
    def call(name, status, agate_table=None):
        if agate_table is None:
            agate_table = dbt.clients.agate_helper.empty_table()

        sql_results[name] = dbt.utils.AttrDict({
            'status': status,
            'data': dbt.clients.agate_helper.as_matrix(agate_table),
            'table': agate_table
        })
        return ''

    return call


def _load_result(sql_results):
    def call(name):
        return sql_results.get(name)

    return call


def _debug_here():
    import sys
    import ipdb
    frame = sys._getframe(3)
    ipdb.set_trace(frame)


def _add_sql_handlers(context):
    sql_results = {}
    return dbt.utils.merge(context, {
        '_sql_results': sql_results,
        'store_result': _store_result(sql_results),
        'load_result': _load_result(sql_results),
    })


def log(msg, info=False):
    if info:
        logger.info(msg)
    else:
        logger.debug(msg)
    return ''


class Var:
    UndefinedVarError = "Required var '{}' not found in config:\nVars "\
                        "supplied to {} = {}"
    _VAR_NOTSET = object()

    def __init__(self, model, context, overrides):
        self.model = model
        self.context = context

        # These are hard-overrides (eg. CLI vars) that should take
        # precedence over context-based var definitions
        self.overrides = overrides

        if model is None:
            # during config parsing we have no model and no local vars
            self.model_name = '<Configuration>'
            local_vars = {}
        else:
            self.model_name = model.name
            local_vars = model.local_vars()

        self.local_vars = dbt.utils.merge(local_vars, overrides)

    def pretty_dict(self, data):
        return json.dumps(data, sort_keys=True, indent=4)

    def get_missing_var(self, var_name):
        pretty_vars = self.pretty_dict(self.local_vars)
        msg = self.UndefinedVarError.format(
            var_name, self.model_name, pretty_vars
        )
        dbt.exceptions.raise_compiler_error(msg, self.model)

    def assert_var_defined(self, var_name, default):
        if var_name not in self.local_vars and default is self._VAR_NOTSET:
            return self.get_missing_var(var_name)

    def get_rendered_var(self, var_name):
        raw = self.local_vars[var_name]
        # if bool/int/float/etc are passed in, don't compile anything
        if not isinstance(raw, str):
            return raw

        return dbt.clients.jinja.get_rendered(raw, self.context)

    def __call__(self, var_name, default=_VAR_NOTSET):
        if var_name in self.local_vars:
            return self.get_rendered_var(var_name)
        elif default is not self._VAR_NOTSET:
            return default
        else:
            return self.get_missing_var(var_name)


def write(node, target_path, subdirectory):
    def fn(payload):
        node.build_path = dbt.writer.write_node(
            node, target_path, subdirectory, payload)
        return ''

    return fn


def render(context, node):
    def fn(string):
        return dbt.clients.jinja.get_rendered(string, context, node)

    return fn


def fromjson(string, default=None):
    try:
        return json.loads(string)
    except ValueError:
        return default


def tojson(value, default=None):
    try:
        return json.dumps(value)
    except ValueError:
        return default


def try_or_compiler_error(model):
    def impl(message_if_exception, func, *args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            dbt.exceptions.raise_compiler_error(message_if_exception, model)
    return impl


def _return(value):
    raise dbt.exceptions.MacroReturn(value)


def get_pytz_module_context():
    context_exports = pytz.__all__

    return {
        name: getattr(pytz, name) for name in context_exports
    }


def get_re_module_context():
    context_exports = re.__all__

    return {
        name: getattr(re, name) for name in context_exports
    }


def get_datetime_module_context():
    context_exports = [
        'date',
        'datetime',
        'time',
        'timedelta',
        'tzinfo'
    ]

    return {
        name: getattr(datetime, name) for name in context_exports
    }


def get_context_modules():
    return {
        'pytz': get_pytz_module_context(),
        'datetime': get_datetime_module_context(),
        're': get_re_module_context(),
    }


def generate_config_context(cli_vars):
    context = {
        'env_var': env_var,
        'modules': get_context_modules(),
    }
    context['var'] = Var(None, context, cli_vars)
    return _add_tracking(context)


def _build_load_agate_table(
    model: Union[ParsedSeedNode, CompiledSeedNode]
) -> Callable[[], agate.Table]:
    def load_agate_table():
        path = model.seed_file_path
        try:
            table = dbt.clients.agate_helper.from_csv(path)
        except ValueError as e:
            dbt.exceptions.raise_compiler_error(str(e))
        table.original_abspath = os.path.abspath(path)
        return table
    return load_agate_table


def generate_base(model, model_dict, config, manifest, source_config,
                  provider, adapter=None):
    """Generate the common aspects of the config dict."""
    if provider is None:
        raise dbt.exceptions.InternalException(
            "Invalid provider given to context: {}".format(provider))

    target_name = config.target_name
    target = config.to_profile_info()
    del target['credentials']
    target.update(config.credentials.to_dict(with_aliases=True))
    target['type'] = config.credentials.type
    target.pop('pass', None)
    target.pop('password', None)
    target['name'] = target_name

    adapter = get_adapter(config)

    context = {'env': target}

    pre_hooks = None
    post_hooks = None

    db_wrapper = provider.DatabaseWrapper(adapter)

    context = dbt.utils.merge(context, {
        "adapter": db_wrapper,
        "api": {
            "Relation": db_wrapper.Relation,
            "Column": adapter.Column,
        },
        "column": adapter.Column,
        "config": provider.Config(model, source_config),
        "database": config.credentials.database,
        "env_var": env_var,
        "exceptions": dbt.exceptions.wrapped_exports(model),
        "execute": provider.execute,
        "flags": dbt.flags,
        "load_agate_table": _build_load_agate_table(model),
        "graph": manifest.flat_graph,
        "log": log,
        "model": model_dict,
        "modules": get_context_modules(),
        "post_hooks": post_hooks,
        "pre_hooks": pre_hooks,
        "ref": provider.ref(db_wrapper, model, config, manifest),
        "return": _return,
        "schema": config.credentials.schema,
        "sql": None,
        "sql_now": adapter.date_function(),
        "source": provider.source(db_wrapper, model, config, manifest),
        "fromjson": fromjson,
        "tojson": tojson,
        "target": target,
        "try_or_compiler_error": try_or_compiler_error(model)
    })
    if os.environ.get('DBT_MACRO_DEBUGGING'):
        context['debug'] = _debug_here

    return context


def modify_generated_context(context, model, config, manifest, provider):
    cli_var_overrides = config.cli_vars

    context = _add_tracking(context)
    context = _add_validation(context)
    context = _add_sql_handlers(context)

    # we make a copy of the context for each of these ^^

    context = _add_macros(context, model, manifest)

    context["write"] = write(model, config.target_path, 'run')
    context["render"] = render(context, model)
    context["var"] = provider.Var(model, context=context,
                                  overrides=cli_var_overrides)
    context['context'] = context

    return context


def generate_execute_macro(model, config, manifest, provider):
    """Internally, macros can be executed like nodes, with some restrictions:

     - they don't have have all values available that nodes do:
        - 'this', 'pre_hooks', 'post_hooks', and 'sql' are missing
        - 'schema' does not use any 'model' information
     - they can't be configured with config() directives
    """
    model_dict = model.to_dict()
    context = generate_base(model, model_dict, config, manifest, None,
                            provider)

    return modify_generated_context(context, model, config, manifest, provider)


def generate_model(model, config, manifest, source_config, provider):
    model_dict = model.to_dict()
    context = generate_base(model, model_dict, config, manifest,
                            source_config, provider)
    # operations (hooks) don't get a 'this'
    if model.resource_type != NodeType.Operation:
        this = context['adapter'].Relation.create_from(config, model)
        context['this'] = this
    # overwrite schema/database if we have them, and hooks + sql
    # the hooks should come in as dicts, at least for the `run_hooks` macro
    # TODO: do we have to preserve this as backwards a compatibility thing?
    context.update({
        'schema': getattr(model, 'schema', context['schema']),
        'database': getattr(model, 'database', context['database']),
        'pre_hooks': [h.to_dict() for h in model.config.pre_hook],
        'post_hooks': [h.to_dict() for h in model.config.post_hook],
        'sql': getattr(model, 'injected_sql', None),
    })

    return modify_generated_context(context, model, config, manifest, provider)


def generate(model, config, manifest, source_config=None, provider=None):
    """
    Not meant to be called directly. Call with either:
        dbt.context.parser.generate
    or
        dbt.context.runtime.generate
    """
    return generate_model(model, config, manifest, source_config, provider)
