import unittest
from unittest import mock

import dbt.flags as flags
from dbt.task.debug import DebugTask

from dbt.adapters.postgres import PostgresAdapter
from dbt.exceptions import ValidationException, DbtConfigError
from dbt.logger import GLOBAL_LOGGER as logger  # noqa
from dbt.parser.results import ParseResult
from psycopg2 import extensions as psycopg2_extensions
from psycopg2 import DatabaseError, Error
import agate

from .utils import config_from_parts_or_dicts, inject_adapter, mock_connection


class TestPostgresAdapter(unittest.TestCase):

    def setUp(self):
        flags.STRICT_MODE = True
        project_cfg = {
            'name': 'X',
            'version': '0.1',
            'profile': 'test',
            'project-root': '/tmp/dbt/does-not-exist',
        }
        profile_cfg = {
            'outputs': {
                'test': {
                    'type': 'postgres',
                    'dbname': 'postgres',
                    'user': 'root',
                    'host': 'thishostshouldnotexist',
                    'pass': 'password',
                    'port': 5432,
                    'schema': 'public'
                }
            },
            'target': 'test'
        }

        self.config = config_from_parts_or_dicts(project_cfg, profile_cfg)
        self._adapter = None

    @property
    def adapter(self):
        if self._adapter is None:
            self._adapter = PostgresAdapter(self.config)
            inject_adapter(self._adapter)
        return self._adapter

    @mock.patch('dbt.adapters.postgres.connections.psycopg2')
    def test_acquire_connection_validations(self, psycopg2):
        try:
            connection = self.adapter.acquire_connection('dummy')
        except ValidationException as e:
            self.fail('got ValidationException: {}'.format(str(e)))
        except BaseException as e:
            self.fail('acquiring connection failed with unknown exception: {}'
                      .format(str(e)))
        self.assertEqual(connection.type, 'postgres')
        psycopg2.connect.assert_called_once()

    @mock.patch('dbt.adapters.postgres.connections.psycopg2')
    def test_acquire_connection(self, psycopg2):
        connection = self.adapter.acquire_connection('dummy')

        self.assertEqual(connection.state, 'open')
        self.assertNotEqual(connection.handle, None)
        psycopg2.connect.assert_called_once()

    def test_cancel_open_connections_empty(self):
        self.assertEqual(len(list(self.adapter.cancel_open_connections())), 0)

    def test_cancel_open_connections_master(self):
        key = self.adapter.connections.get_thread_identifier()
        self.adapter.connections.thread_connections[key] = mock_connection('master')
        self.assertEqual(len(list(self.adapter.cancel_open_connections())), 0)

    def test_cancel_open_connections_single(self):
        master = mock_connection('master')
        model = mock_connection('model')
        key = self.adapter.connections.get_thread_identifier()
        model.handle.get_backend_pid.return_value = 42
        self.adapter.connections.thread_connections.update({
            key: master,
            1: model,
        })
        with mock.patch.object(self.adapter.connections, 'add_query') as add_query:
            query_result = mock.MagicMock()
            add_query.return_value = (None, query_result)

            self.assertEqual(len(list(self.adapter.cancel_open_connections())), 1)

            add_query.assert_called_once_with('select pg_terminate_backend(42)')

        master.handle.get_backend_pid.assert_not_called()

    @mock.patch('dbt.adapters.postgres.connections.psycopg2')
    def test_default_keepalive(self, psycopg2):
        connection = self.adapter.acquire_connection('dummy')

        psycopg2.connect.assert_called_once_with(
            dbname='postgres',
            user='root',
            host='thishostshouldnotexist',
            password='password',
            port=5432,
            connect_timeout=10)

    @mock.patch('dbt.adapters.postgres.connections.psycopg2')
    def test_changed_keepalive(self, psycopg2):
        self.config.credentials = self.config.credentials.replace(keepalives_idle=256)
        connection = self.adapter.acquire_connection('dummy')

        psycopg2.connect.assert_called_once_with(
            dbname='postgres',
            user='root',
            host='thishostshouldnotexist',
            password='password',
            port=5432,
            connect_timeout=10,
            keepalives_idle=256)

    @mock.patch('dbt.adapters.postgres.connections.psycopg2')
    def test_search_path(self, psycopg2):
        self.config.credentials = self.config.credentials.replace(search_path="test")
        connection = self.adapter.acquire_connection('dummy')

        psycopg2.connect.assert_called_once_with(
            dbname='postgres',
            user='root',
            host='thishostshouldnotexist',
            password='password',
            port=5432,
            connect_timeout=10,
            options="-c search_path=test")

    @mock.patch('dbt.adapters.postgres.connections.psycopg2')
    def test_schema_with_space(self, psycopg2):
        self.config.credentials = self.config.credentials.replace(search_path="test test")
        connection = self.adapter.acquire_connection('dummy')

        psycopg2.connect.assert_called_once_with(
            dbname='postgres',
            user='root',
            host='thishostshouldnotexist',
            password='password',
            port=5432,
            connect_timeout=10,
            options="-c search_path=test\ test")

    @mock.patch('dbt.adapters.postgres.connections.psycopg2')
    def test_set_zero_keepalive(self, psycopg2):
        self.config.credentials = self.config.credentials.replace(keepalives_idle=0)
        connection = self.adapter.acquire_connection('dummy')

        psycopg2.connect.assert_called_once_with(
            dbname='postgres',
            user='root',
            host='thishostshouldnotexist',
            password='password',
            port=5432,
            connect_timeout=10)

    @mock.patch.object(PostgresAdapter, 'execute_macro')
    def test_get_catalog_various_schemas(self, mock_execute):
        column_names = ['table_database', 'table_schema', 'table_name']
        rows = [
            ('dbt', 'foo', 'bar'),
            ('dbt', 'FOO', 'baz'),
            ('dbt', None, 'bar'),
            ('dbt', 'quux', 'bar'),
            ('dbt', 'skip', 'bar'),
        ]
        mock_execute.return_value = agate.Table(rows=rows,
                                                column_names=column_names)

        mock_manifest = mock.MagicMock()
        mock_manifest.get_used_schemas.return_value = {('dbt', 'foo'),
                                                       ('dbt', 'quux')}

        catalog = self.adapter.get_catalog(mock_manifest)
        self.assertEqual(
            set(map(tuple, catalog)),
            {('dbt', 'foo', 'bar'), ('dbt', 'FOO', 'baz'), ('dbt', 'quux', 'bar')}
        )


class TestConnectingPostgresAdapter(unittest.TestCase):
    def setUp(self):
        flags.STRICT_MODE = False

        self.target_dict = {
            'type': 'postgres',
            'dbname': 'postgres',
            'user': 'root',
            'host': 'thishostshouldnotexist',
            'pass': 'password',
            'port': 5432,
            'schema': 'public'
        }

        profile_cfg = {
            'outputs': {
                'test': self.target_dict,
            },
            'target': 'test'
        }
        project_cfg = {
            'name': 'X',
            'version': '0.1',
            'profile': 'test',
            'project-root': '/tmp/dbt/does-not-exist',
            'quoting': {
                'identifier': False,
                'schema': True,
            }
        }

        self.config = config_from_parts_or_dicts(project_cfg, profile_cfg)

        self.handle = mock.MagicMock(spec=psycopg2_extensions.connection)
        self.cursor = self.handle.cursor.return_value
        self.mock_execute = self.cursor.execute
        self.patcher = mock.patch('dbt.adapters.postgres.connections.psycopg2')
        self.psycopg2 = self.patcher.start()
        # there must be a better way to do this...
        self.psycopg2.DatabaseError = DatabaseError
        self.psycopg2.Error = Error

        self.psycopg2.connect.return_value = self.handle
        self.adapter = PostgresAdapter(self.config)
        self.adapter.acquire_connection()
        inject_adapter(self.adapter)

        self.load_patch = mock.patch('dbt.loader.make_parse_result')
        self.mock_parse_result = self.load_patch.start()
        self.mock_parse_result.return_value = ParseResult.rpc()

    def tearDown(self):
        # we want a unique self.handle every time.
        self.adapter.cleanup_connections()
        self.patcher.stop()
        self.load_patch.stop()

    def test_quoting_on_drop_schema(self):
        self.adapter.drop_schema(database='postgres', schema='test_schema')

        self.mock_execute.assert_has_calls([
            mock.call('drop schema if exists "test_schema" cascade', None)
        ])

    def test_quoting_on_drop(self):
        relation = self.adapter.Relation.create(
            database='postgres',
            schema='test_schema',
            identifier='test_table',
            type='table',
            quote_policy=self.adapter.config.quoting,
        )
        self.adapter.drop_relation(relation)
        self.mock_execute.assert_has_calls([
            mock.call('drop table if exists "postgres"."test_schema".test_table cascade', None)
        ])

    def test_quoting_on_truncate(self):
        relation = self.adapter.Relation.create(
            database='postgres',
            schema='test_schema',
            identifier='test_table',
            type='table',
            quote_policy=self.adapter.config.quoting,
        )
        self.adapter.truncate_relation(relation)
        self.mock_execute.assert_has_calls([
            mock.call('truncate table "postgres"."test_schema".test_table', None)
        ])

    def test_quoting_on_rename(self):
        from_relation = self.adapter.Relation.create(
            database='postgres',
            schema='test_schema',
            identifier='table_a',
            type='table',
            quote_policy=self.adapter.config.quoting,
        )
        to_relation = self.adapter.Relation.create(
            database='postgres',
            schema='test_schema',
            identifier='table_b',
            type='table',
            quote_policy=self.adapter.config.quoting,
        )

        self.adapter.rename_relation(
            from_relation=from_relation,
            to_relation=to_relation
        )
        self.mock_execute.assert_has_calls([
            mock.call('alter table "postgres"."test_schema".table_a rename to table_b', None)
        ])

    def test_debug_connection_ok(self):
        DebugTask.validate_connection(self.target_dict)
        self.mock_execute.assert_has_calls([
            mock.call('select 1 as id', None)
        ])

    def test_debug_connection_fail_nopass(self):
        del self.target_dict['pass']
        with self.assertRaises(DbtConfigError):
            DebugTask.validate_connection(self.target_dict)

        def test_connection_fail_select(self):
            self.mock_execute.side_effect = DatabaseError()
            with self.assertRaises(DbtConfigError):
                DebugTask.validate_connection(self.target_dict)
            self.mock_execute.assert_has_calls([
                mock.call('select 1 as id', None)
            ])

