from test.integration.base import DBTIntegrationTest,  use_profile
import os


class TestDebug(DBTIntegrationTest):
    @property
    def schema(self):
        return 'dbt_warehouse_050'

    @staticmethod
    def dir(value):
        return os.path.normpath(value)

    @property
    def models(self):
        return self.dir('models')

    @use_profile('snowflake')
    def test_snowflake_override_ok(self):
        self.run_dbt([
            'run',
            '--models', 'override_warehouse', 'expected_warehouse',
        ])
        self.assertManyRelationsEqual([['OVERRIDE_WAREHOUSE'], ['EXPECTED_WAREHOUSE']])

    @use_profile('snowflake')
    def test_snowflake_override_noexist(self):
        self.run_dbt(['run', '--models', 'invalid_warehouse'], expect_pass=False)
