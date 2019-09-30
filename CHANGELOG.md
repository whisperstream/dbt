## dbt 0.15.0 (TBD)

### Breaking changes
 - Cache management changes:
  - Materialization macros should now return a dictionary {"relations": [...]}, with the list containing all relations that have been added, in order to add them to the cache. The default behavior is to still add the materialization's model to the cache.
  - Materializations that perform drops via direct "drop" statements must call `adapter.cache_dropped`
      - `adapter.drop_relation` already does this
  - Materializations that perform renames via direct "alter table" statements must call `adapter.cache_renamed`
      - `adapter.rename_relation` already does this

## dbt 0.14.3 (Unreleased)

This is a bugfix release.

### Fixes:
- Fix for `dictionary changed size during iteration` race condition ([#1740](https://github.com/fishtown-analytics/dbt/issues/1740), [#1750](https://github.com/fishtown-analytics/dbt/pull/1750))

### Under the hood:
- Provide a programmatic method for validating profile targets ([#1754](https://github.com/fishtown-analytics/dbt/issues/1754), [#1775](https://github.com/fishtown-analytics/dbt/pull/1775))

>>>>>>> dev/0.14.3

## dbt 0.14.2 (September 13, 2019)

### Overview

This is a bugfix release.

### Fixes:
- Fix for dbt hanging at the end of execution in `dbt source snapshot-freshness` tasks ([#1728](https://github.com/fishtown-analytics/dbt/issues/1728), [#1729](https://github.com/fishtown-analytics/dbt/pull/1729))
- Fix for broken "packages" and "tags" selector dropdowns in the dbt Documentation website ([docs#47](https://github.com/fishtown-analytics/dbt-docs/issues/47), [#1726](https://github.com/fishtown-analytics/dbt/pull/1726))


## dbt 0.14.1 (September 3, 2019)

### Overview

This is primarily a bugfix release which contains a few minor improvements too. Note: this release includes an important change in how the `check` snapshot strategy works. See [#1614](https://github.com/fishtown-analytics/dbt/pull/1614) for more information. If you are using snapshots with the `check` strategy on dbt v0.14.0, it is strongly recommended that you upgrade to 0.14.1 at your soonest convenience.

### Breaking changes
 - The undocumented `macros` attribute was removed from the `graph` context variable ([#1615](https://github.com/fishtown-analytics/dbt/pull/1615))

### Features:
 - Summarize warnings at the end of dbt runs ([#1597](https://github.com/fishtown-analytics/dbt/issues/1597), [#1654](https://github.com/fishtown-analytics/dbt/pull/1654))
 - Speed up catalog generation on postgres by using avoiding use of the `information_schema` ([#1540](https://github.com/fishtown-analytics/dbt/pull/1540))
 - Docs site updates ([#1621](https://github.com/fishtown-analytics/dbt/issues/1621))
   - Fix for incorrect node selection logic in DAG view ([docs#38](https://github.com/fishtown-analytics/dbt-docs/pull/38))
   - Update page title, meta tags, and favicon ([docs#39](https://github.com/fishtown-analytics/dbt-docs/pull/39))
   - Bump the version of `dbt-styleguide`, changing file tree colors from orange to black :)
 - Add environment variables for macro debugging flags ([#1628](https://github.com/fishtown-analytics/dbt/issues/1628), [#1629](https://github.com/fishtown-analytics/dbt/pull/1629))
 - Speed up node selection by making it linear, rather than quadratic, in complexity ([#1611](https://github.com/fishtown-analytics/dbt/issues/1611), [#1615](https://github.com/fishtown-analytics/dbt/pull/1615))
 - Specify the `application` field in Snowflake connections ([#1622](https://github.com/fishtown-analytics/dbt/issues/1622), [#1623](https://github.com/fishtown-analytics/dbt/pull/1623))
 - Add support for clustering on Snowflake ([#634](https://github.com/fishtown-analytics/dbt/issues/634), [#1591](https://github.com/fishtown-analytics/dbt/pull/1591), [#1689](https://github.com/fishtown-analytics/dbt/pull/1689)) ([docs](https://docs.getdbt.com/docs/snowflake-configs#section-configuring-table-clustering))
 - Add support for job priority on BigQuery ([#1456](https://github.com/fishtown-analytics/dbt/issues/1456), [#1673](https://github.com/fishtown-analytics/dbt/pull/1673)) ([docs](https://docs.getdbt.com/docs/profile-bigquery#section-priority))
 - Add `node.config` and `node.tags` to the `generate_schema_name` and `generate_alias_name` macro context ([#1700](https://github.com/fishtown-analytics/dbt/issues/1700), [#1701](https://github.com/fishtown-analytics/dbt/pull/1701))

### Fixes:
 - Fix for reused `check_cols` values in snapshots ([#1614](https://github.com/fishtown-analytics/dbt/pull/1614), [#1709](https://github.com/fishtown-analytics/dbt/pull/1709))
 - Fix for rendering column descriptions in sources ([#1619](https://github.com/fishtown-analytics/dbt/issues/1619), [#1633](https://github.com/fishtown-analytics/dbt/pull/1633))
 - Fix for `is_incremental()` returning True for models that are not materialized as incremental models ([#1249](https://github.com/fishtown-analytics/dbt/issues/1249), [#1608](https://github.com/fishtown-analytics/dbt/pull/1608))
 - Fix for serialization of BigQuery results which contain nested or repeated records ([#1626](https://github.com/fishtown-analytics/dbt/issues/1626), [#1638](https://github.com/fishtown-analytics/dbt/pull/1638))
 - Fix for loading seed files which contain non-ascii characters ([#1632](https://github.com/fishtown-analytics/dbt/issues/1632), [#1644](https://github.com/fishtown-analytics/dbt/pull/1644))
 - Fix for creation of user cookies in incorrect directories when `--profile-dir` or `$DBT_PROFILES_DIR` is provided ([#1645](https://github.com/fishtown-analytics/dbt/issues/1645), [#1656](https://github.com/fishtown-analytics/dbt/pull/1656))
 - Fix for error handling when transactions are being rolled back ([#1647](https://github.com/fishtown-analytics/dbt/pull/1647))
 - Fix for incorrect references to `dbt.exceptions` in jinja code ([#1569](https://github.com/fishtown-analytics/dbt/issues/1569), [#1609](https://github.com/fishtown-analytics/dbt/pull/1609))
 - Fix for duplicated schema creation due to case-sensitive comparison ([#1651](https://github.com/fishtown-analytics/dbt/issues/1651), [#1663](https://github.com/fishtown-analytics/dbt/pull/1663))
 - Fix for "schema stub" created automatically by dbt ([#913](https://github.com/fishtown-analytics/dbt/issues/913), [#1663](https://github.com/fishtown-analytics/dbt/pull/1663))
 - Fix for incremental merge query on old versions of postgres (<=9.6) ([#1665](https://github.com/fishtown-analytics/dbt/issues/1665), [#1666](https://github.com/fishtown-analytics/dbt/pull/1666))
 - Fix for serializing results of queries which return `TIMESTAMP_TZ` columns on Snowflake in the RPC server ([#1670](https://github.com/fishtown-analytics/dbt/pull/1670))
 - Fix typo in InternalException ([#1640](https://github.com/fishtown-analytics/dbt/issues/1640), [#1672](https://github.com/fishtown-analytics/dbt/pull/1672))
 - Fix typo in CLI help for snapshot migration subcommand ([#1664](https://github.com/fishtown-analytics/dbt/pull/1664))
 - Fix for error handling logic when empty queries are submitted on Snowflake ([#1693](https://github.com/fishtown-analytics/dbt/issues/1693), [#1694](https://github.com/fishtown-analytics/dbt/pull/1694))
 - Fix for non-atomic column expansion logic in Snowflake incremental models and snapshots ([#1687](https://github.com/fishtown-analytics/dbt/issues/1687), [#1690](https://github.com/fishtown-analytics/dbt/pull/1690))
 - Fix for unprojected `count(*)` expression injected by custom data tests ([#1688](https://github.com/fishtown-analytics/dbt/pull/1688))
 - Fix for `dbt run` and `dbt docs generate` commands when running against Panoply Redshift ([#1479](https://github.com/fishtown-analytics/dbt/issues/1479), [#1686](https://github.com/fishtown-analytics/dbt/pull/1686))


 ### Contributors:
Thanks for your contributions to dbt!

- [@levimalott](https://github.com/levimalott) ([#1647](https://github.com/fishtown-analytics/dbt/pull/1647))
- [@aminamos](https://github.com/aminamos) ([#1609](https://github.com/fishtown-analytics/dbt/pull/1609))
- [@elexisvenator](https://github.com/elexisvenator) ([#1540](https://github.com/fishtown-analytics/dbt/pull/1540))
- [@edmundyan](https://github.com/edmundyan) ([#1663](https://github.com/fishtown-analytics/dbt/pull/1663))
- [@vitorbaptista](https://github.com/vitorbaptista) ([#1664](https://github.com/fishtown-analytics/dbt/pull/1664))
- [@sjwhitworth](https://github.com/sjwhitworth) ([#1672](https://github.com/fishtown-analytics/dbt/pull/1672), [#1673](https://github.com/fishtown-analytics/dbt/pull/1673))
- [@mikaelene](https://github.com/mikaelene) ([#1688](https://github.com/fishtown-analytics/dbt/pull/1688), [#1709](https://github.com/fishtown-analytics/dbt/pull/1709))
- [@bastienboutonnet](https://github.com/bastienboutonnet) ([#1591](https://github.com/fishtown-analytics/dbt/pull/1591), [#1689](https://github.com/fishtown-analytics/dbt/pull/1689))



## dbt 0.14.0 - Wilt Chamberlain (July 10, 2019)

### Overview
- Replace Archives with Snapshots ([docs](https://docs.getdbt.com/v0.14/docs/snapshots), [migration guide](https://docs.getdbt.com/v0.14/docs/upgrading-to-014))
- Add three new top-level commands:
  - `dbt ls` ([docs](https://docs.getdbt.com/v0.14/docs/list))
  - `dbt run-operation` ([docs](https://docs.getdbt.com/v0.14/docs/run-operation))
  - `dbt rpc` ([docs](https://docs.getdbt.com/v0.14/docs/rpc))
- Support the specification of severity levels for schema and data tests ([docs](https://docs.getdbt.com/v0.14/docs/testing#section-test-severity))
- Many new quality of life improvements and bugfixes

### Breaking changes
- Stub out adapter methods at parse-time to speed up parsing ([#1413](https://github.com/fishtown-analytics/dbt/pull/1413))
- Removed support for the `--non-destructive` flag ([#1419](https://github.com/fishtown-analytics/dbt/pull/1419), [#1415](https://github.com/fishtown-analytics/dbt/issues/1415))
- Removed support for the `sql_where` config to incremental models ([#1408](https://github.com/fishtown-analytics/dbt/pull/1408), [#1351](https://github.com/fishtown-analytics/dbt/issues/1351))
- Changed `expand_target_column_types` to take a Relation instead of a string ([#1478](https://github.com/fishtown-analytics/dbt/pull/1478))
- Replaced Archives with Snapshots
  - Normalized meta-column names in Snapshot tables ([#1361](https://github.com/fishtown-analytics/dbt/pull/1361), [#251](https://github.com/fishtown-analytics/dbt/issues/251))

### Features
- Add `run-operation` command which invokes macros directly from the CLI ([#1328](https://github.com/fishtown-analytics/dbt/pull/1328)) ([docs](https://docs.getdbt.com/v0.14/docs/run-operation))
- Add a `dbt ls` command which lists resources in your project ([#1436](https://github.com/fishtown-analytics/dbt/pull/1436), [#467](https://github.com/fishtown-analytics/dbt/issues/467)) ([docs](https://docs.getdbt.com/v0.14/docs/list))
- Add Snapshots, an improvement over Archives ([#1361](https://github.com/fishtown-analytics/dbt/pull/1361), [#1175](https://github.com/fishtown-analytics/dbt/issues/1175)) ([docs](https://docs.getdbt.com/v0.14/docs/snapshots))
  - Add the 'check' snapshot strategy ([#1361](https://github.com/fishtown-analytics/dbt/pull/1361), [#706](https://github.com/fishtown-analytics/dbt/issues/706))
  - Support Snapshots across logical databases ([#1455](https://github.com/fishtown-analytics/dbt/issues/1455))
  - Implement Snapshots using a merge statement where supported ([#1478](https://github.com/fishtown-analytics/dbt/pull/1478))
  - Support Snapshot selection using `--select` ([#1520](https://github.com/fishtown-analytics/dbt/pull/1520), [#1512](https://github.com/fishtown-analytics/dbt/issues/1512))
- Add an RPC server via `dbt rpc` ([#1301](https://github.com/fishtown-analytics/dbt/pull/1301), [#1274](https://github.com/fishtown-analytics/dbt/issues/1274)) ([docs](https://docs.getdbt.com/v0.14/docs/rpc))
  - Add `ps` and `kill` commands to the rpc server ([#1380](https://github.com/fishtown-analytics/dbt/pull/1380/), [#1369](https://github.com/fishtown-analytics/dbt/issues/1369), [#1370](https://github.com/fishtown-analytics/dbt/issues/1370))
  - Add support for ephemeral nodes to the rpc server ([#1373](https://github.com/fishtown-analytics/dbt/pull/1373), [#1368](https://github.com/fishtown-analytics/dbt/issues/1368))
  - Add support for inline macros to the rpc server ([#1375](https://github.com/fishtown-analytics/dbt/pull/1375), [#1372](https://github.com/fishtown-analytics/dbt/issues/1372), [#1348](https://github.com/fishtown-analytics/dbt/pull/1348))
  - Improve error handling in the rpc server ([#1341](https://github.com/fishtown-analytics/dbt/pull/1341), [#1309](https://github.com/fishtown-analytics/dbt/issues/1309), [#1310](https://github.com/fishtown-analytics/dbt/issues/1310))
- Made printer width configurable ([#1026](https://github.com/fishtown-analytics/dbt/issues/1026), [#1247](https://github.com/fishtown-analytics/dbt/pull/1247)) ([docs](https://docs.getdbt.com/v0.14/docs/configure-your-profile#section-additional-profile-configurations))
- Retry package downloads from the hub.getdbt.com ([#1451](https://github.com/fishtown-analytics/dbt/issues/1451), [#1491](https://github.com/fishtown-analytics/dbt/pull/1491))
- Add a test "severity" level, presented as a keyword argument to schema tests ([#1410](https://github.com/fishtown-analytics/dbt/pull/1410), [#1005](https://github.com/fishtown-analytics/dbt/issues/1005)) ([docs](https://docs.getdbt.com/v0.14/docs/testing#section-test-severity))
- Add a `generate_alias_name` macro to configure alias names dynamically ([#1363](https://github.com/fishtown-analytics/dbt/pull/1363)) ([docs](https://docs.getdbt.com/v0.14/docs/using-custom-aliases#section-generate_alias_name))
- Add a `node` argument to `generate_schema_name` to configure schema names dynamically ([#1483](https://github.com/fishtown-analytics/dbt/pull/1483), [#1463](https://github.com/fishtown-analytics/dbt/issues/1463)) ([docs](https://docs.getdbt.com/v0.14/docs/using-custom-schemas#section-generate_schema_name-arguments))
- Use `create or replace` on Snowflake to rebuild tables and views atomically ([#1101](https://github.com/fishtown-analytics/dbt/issues/1101), [#1409](https://github.com/fishtown-analytics/dbt/pull/1409))
- Use `merge` statement for incremental models on Snowflake ([#1414](https://github.com/fishtown-analytics/dbt/issues/1414), [#1307](https://github.com/fishtown-analytics/dbt/pull/1307), [#1409](https://github.com/fishtown-analytics/dbt/pull/1409)) ([docs](https://docs.getdbt.com/v0.14/docs/snowflake-configs#section-merge-behavior-incremental-models-))
- Add support seed CSV files that start with a UTF-8 Byte Order Mark (BOM) ([#1452](https://github.com/fishtown-analytics/dbt/pull/1452), [#1177](https://github.com/fishtown-analytics/dbt/issues/1177))
- Add a warning when git packages are not pinned to a version ([#1453](https://github.com/fishtown-analytics/dbt/pull/1453), [#1446](https://github.com/fishtown-analytics/dbt/issues/1446))
- Add logging for `on-run-start` and `on-run-end hooks` to console output ([#1440](https://github.com/fishtown-analytics/dbt/pull/1440), [#696](https://github.com/fishtown-analytics/dbt/issues/696))
- Add modules and tracking information to the rendering context for configuration files ([#1441](https://github.com/fishtown-analytics/dbt/pull/1441), [#1320](https://github.com/fishtown-analytics/dbt/issues/1320))
- Add support for `null` vars, and distinguish `null` vars from unset vars ([#1426](https://github.com/fishtown-analytics/dbt/pull/1426), [#608](https://github.com/fishtown-analytics/dbt/issues/608))
- Add support for the `search_path` configuration in Postgres/Redshift profiles ([#1477](https://github.com/fishtown-analytics/dbt/issues/1477), [#1476](https://github.com/fishtown-analytics/dbt/pull/1476)) ([docs (postgres)](https://docs.getdbt.com/v0.14/docs/profile-postgres), [docs (redshift)](https://docs.getdbt.com/v0.14/docs/profile-redshift))
- Add support for persisting documentation as `descriptions` for tables and views on BigQuery ([#1031](https://github.com/fishtown-analytics/dbt/issues/1031), [#1285](https://github.com/fishtown-analytics/dbt/pull/1285)) ([docs](https://docs.getdbt.com/v0.14/docs/bigquery-configs#section-persisting-model-descriptions))
- Add a `--project-dir` path which will invoke dbt in the specified directory ([#1549](https://github.com/fishtown-analytics/dbt/pull/1549), [#1544](https://github.com/fishtown-analytics/dbt/issues/1544))

### dbt docs Changes
- Add searching by tag name ([#32](https://github.com/fishtown-analytics/dbt-docs/pull/32))
- Add context menu link to export graph viz as a PNG ([#34](https://github.com/fishtown-analytics/dbt-docs/pull/34))
- Fix for clicking models in left-nav while search results are open ([#31](https://github.com/fishtown-analytics/dbt-docs/pull/31))

### Fixes
- Fix for unduly long timeouts when anonymous event tracking is blocked ([#1445](https://github.com/fishtown-analytics/dbt/pull/1445), [#1063](https://github.com/fishtown-analytics/dbt/issues/1063))
- Fix for error with mostly-duplicate git urls in packages, picking the one that came first. ([#1428](https://github.com/fishtown-analytics/dbt/pull/1428), [#1084](https://github.com/fishtown-analytics/dbt/issues/1084))
- Fix for unrendered `description` field as jinja in top-level Source specification ([#1484](https://github.com/fishtown-analytics/dbt/issues/1484), [#1494](https://github.com/fishtown-analytics/dbt/issues/1494))
- Fix for API error when very large temp tables are created in BigQuery ([#1423](https://github.com/fishtown-analytics/dbt/issues/1423), [#1478](https://github.com/fishtown-analytics/dbt/pull/1478))
- Fix for compiler errors that occurred if jinja code was present outside of a docs blocks in .md files ([#1513](https://github.com/fishtown-analytics/dbt/pull/1513), [#988](https://github.com/fishtown-analytics/dbt/issues/988))
- Fix `TEXT` handling on postgres and redshift ([#1420](https://github.com/fishtown-analytics/dbt/pull/1420), [#781](https://github.com/fishtown-analytics/dbt/issues/781))
- Fix for compiler error when vars are undefined but only used in disabled models ([#1429](https://github.com/fishtown-analytics/dbt/pull/1429), [#434](https://github.com/fishtown-analytics/dbt/issues/434))
- Improved the error message when iterating over the results of a macro that doesn't exist ([#1425](https://github.com/fishtown-analytics/dbt/pull/1425), [#1424](https://github.com/fishtown-analytics/dbt/issues/1424))
- Improved the error message when tests have invalid parameter definitions ([#1427](https://github.com/fishtown-analytics/dbt/pull/1427), [#1325](https://github.com/fishtown-analytics/dbt/issues/1325))
- Improved the error message when a user tries to archive a non-existent table ([#1361](https://github.com/fishtown-analytics/dbt/pull/1361), [#1066](https://github.com/fishtown-analytics/dbt/issues/1066))
- Fix for archive logic which tried to create already-existing destination schemas ([#1398](https://github.com/fishtown-analytics/dbt/pull/1398), [#758](https://github.com/fishtown-analytics/dbt/issues/758))
- Fix for incorrect error codes when Operations exit with an error ([#1406](https://github.com/fishtown-analytics/dbt/pull/1406), [#1377](https://github.com/fishtown-analytics/dbt/issues/1377))
- Fix for missing compiled SQL when the rpc server encounters a database error ([#1381](https://github.com/fishtown-analytics/dbt/pull/1381), [#1371](https://github.com/fishtown-analytics/dbt/issues/1371))
- Fix for broken link in the profile.yml generated by `dbt init` ([#1366](https://github.com/fishtown-analytics/dbt/pull/1366), [#1344](https://github.com/fishtown-analytics/dbt/issues/1344))
- Fix the sample test.env file's redshift password field ([#1364](https://github.com/fishtown-analytics/dbt/pull/1364))
- Fix collisions on models running concurrently that have duplicate names but have distinguishing aliases ([#1342](https://github.com/fishtown-analytics/dbt/pull/1342), [#1321](https://github.com/fishtown-analytics/dbt/issues/1321))
- Fix for a bad error message when a `version` is missing from a package spec in `packages.yml` ([#1551](https://github.com/fishtown-analytics/dbt/pull/1551), [#1546](https://github.com/fishtown-analytics/dbt/issues/1546))
- Fix for wrong package scope when the two-arg method of `ref` is used ([#1515](https://github.com/fishtown-analytics/dbt/pull/1515), [#1504](https://github.com/fishtown-analytics/dbt/issues/1504))
- Fix missing import in test suite ([#1572](https://github.com/fishtown-analytics/dbt/pull/1572))
- Fix for a Snowflake error when an external table exists in a schema that dbt operates on ([#1571](https://github.com/fishtown-analytics/dbt/pull/1571), [#1505](https://github.com/fishtown-analytics/dbt/issues/1505))


### Under the hood
- Use pytest for tests ([#1417](https://github.com/fishtown-analytics/dbt/pull/1417))
- Use flake8 for linting ([#1361](https://github.com/fishtown-analytics/dbt/pull/1361), [#1333](https://github.com/fishtown-analytics/dbt/issues/1333))
- Added a flag for wrapping models and tests in jinja blocks ([#1407](https://github.com/fishtown-analytics/dbt/pull/1407), [#1400](https://github.com/fishtown-analytics/dbt/issues/1400))
- Connection management: Bind connections threads rather than to names ([#1336](https://github.com/fishtown-analytics/dbt/pull/1336), [#1312](https://github.com/fishtown-analytics/dbt/issues/1312))
- Add deprecation warning for dbt users on Python2 ([#1534](https://github.com/fishtown-analytics/dbt/pull/1534), [#1531](https://github.com/fishtown-analytics/dbt/issues/1531))
- Upgrade networkx to v2.x ([#1509](https://github.com/fishtown-analytics/dbt/pull/1509), [#1496](https://github.com/fishtown-analytics/dbt/issues/1496))
- Anonymously track adapter type and rpc requests when tracking is enabled ([#1575](https://github.com/fishtown-analytics/dbt/pull/1575), [#1574](https://github.com/fishtown-analytics/dbt/issues/1574))
- Fix for test warnings and general test suite cleanup ([#1578](https://github.com/fishtown-analytics/dbt/pull/1578))

### Contributors:
Over a dozen contributors wrote code for this release of dbt! Thanks for taking the time, and nice work y'all! :)

- [@nydnarb](https://github.com/nydnarb) ([#1363](https://github.com/fishtown-analytics/dbt/issues/1363))
- [@emilieschario](https://github.com/emilieschario) ([#1366](https://github.com/fishtown-analytics/dbt/pull/1366))
- [@bastienboutonnet](https://github.com/bastienboutonnet) ([#1409](https://github.com/fishtown-analytics/dbt/pull/1409))
- [@kasanchez](https://github.com/kasanchez) ([#1247](https://github.com/fishtown-analytics/dbt/pull/1247))
- [@Blakewell](https://github.com/Blakewell) ([#1307](https://github.com/fishtown-analytics/dbt/pull/1307))
- [@buremba](https://github.com/buremba) ([#1476](https://github.com/fishtown-analytics/dbt/pull/1476))
- [@darrenhaken](https://github.com/darrenhaken) ([#1285](https://github.com/fishtown-analytics/dbt/pull/1285))
- [@tbescherer](https://github.com/tbescherer) ([#1504](https://github.com/fishtown-analytics/dbt/issues/1504))
- [@heisencoder](https://github.com/heisencoder) ([#1509](https://github.com/fishtown-analytics/dbt/pull/1509), [#1549](https://github.com/fishtown-analytics/dbt/pull/1549). [#1578](https://github.com/fishtown-analytics/dbt/pull/1578))
- [@cclauss](https://github.com/cclauss) ([#1572](https://github.com/fishtown-analytics/dbt/pull/1572))
- [@josegalarza](https://github.com/josegalarza) ([#1571](https://github.com/fishtown-analytics/dbt/pull/1571))
- [@rmgpinto](https://github.com/rmgpinto) ([docs#31](https://github.com/fishtown-analytics/dbt-docs/pull/31), [docs#32](https://github.com/fishtown-analytics/dbt-docs/pull/32))
- [@groodt](https://github.com/groodt) ([docs#34](https://github.com/fishtown-analytics/dbt-docs/pull/34))


## dbt 0.13.1 (May 13, 2019)

### Overview
This is a bugfix release.

### Bugfixes
- Add "MaterializedView" relation type to the Snowflake adapter ([#1430](https://github.com/fishtown-analytics/dbt/issues/1430), [#1432](https://github.com/fishtown-analytics/dbt/pull/1432)) ([@adriank-convoy](https://github.com/adriank-convoy))
- Quote databases properly ([#1396](https://github.com/fishtown-analytics/dbt/issues/1396), [#1402](https://github.com/fishtown-analytics/dbt/pull/1402))
- Use "ilike" instead of "=" for database equality when listing schemas ([#1411](https://github.com/fishtown-analytics/dbt/issues/1411), [#1412](https://github.com/fishtown-analytics/dbt/pull/1412))
- Pass the model name along in get_relations ([#1384](https://github.com/fishtown-analytics/dbt/issues/1384), [#1388](https://github.com/fishtown-analytics/dbt/pull/1388))
- Add logging to dbt clean ([#1261](https://github.com/fishtown-analytics/dbt/issues/1261), [#1383](https://github.com/fishtown-analytics/dbt/pull/1383), [#1391](https://github.com/fishtown-analytics/dbt/pull/1391)) ([@emilieschario](https://github.com/emilieschario))

### dbt Docs
- Search by columns ([dbt-docs#23](https://github.com/fishtown-analytics/dbt-docs/pull/23)) ([rmgpinto](https://github.com/rmgpinto))
- Support @ selector ([dbt-docs#27](https://github.com/fishtown-analytics/dbt-docs/pull/27))
- Fix number formatting on Snowflake and BQ in table stats ([dbt-docs#28](https://github.com/fishtown-analytics/dbt-docs/pull/28))

### Contributors:
Thanks for your contributions to dbt!

- [@emilieschario](https://github.com/emilieschario)
- [@adriank-convoy](https://github.com/adriank-convoy)
- [@rmgpinto](https://github.com/rmgpinto)


## dbt 0.13.0 - Stephen Girard (March 21, 2019)

### Overview

This release provides [a stable API for building new adapters](https://docs.getdbt.com/v0.13/docs/building-a-new-adapter) and reimplements dbt's adapters as "plugins". Additionally, a new adapter for [Presto](https://github.com/fishtown-analytics/dbt-presto) was added using this architecture. Beyond adapters, this release of dbt also includes [Sources](https://docs.getdbt.com/v0.13/docs/using-sources) which can be used to document and test source data tables. See the full list of features added in 0.13.0 below.

### Breaking Changes
- version 1 schema.yml specs are no longer implemented. Please use the version 2 spec instead ([migration guide](https://docs.getdbt.com/docs/upgrading-from-0-10-to-0-11#section-schema-yml-v2-syntax))
- `{{this}}` is no longer implemented for `on-run-start` and `on-run-end` hooks. Use `{{ target }}` or an [`on-run-end` context variable](https://docs.getdbt.com/docs/on-run-end-context#section-schemas) instead ([#1176](https://github.com/fishtown-analytics/dbt/pull/1176), implementing [#878](https://github.com/fishtown-analytics/dbt/issues/878))
- A number of materialization-specific adapter methods have changed in breaking ways. If you use these adapter methods in your macros or materializations, you may need to update your code accordingly.
  - query_for_existing - **removed**, use [get_relation](https://docs.getdbt.com/v0.13/reference#adapter-get-relation) instead.
  - [get_missing_columns](https://docs.getdbt.com/v0.13/reference#adapter-get-missing-columns) - changed to take `Relation`s instead of schemas and identifiers
  - [expand_target_column_types](https://docs.getdbt.com/v0.13/reference#adapter-expand-target-column-types) - changed to take a `Relation` instead of schema, identifier
  - [get_relation](https://docs.getdbt.com/v0.13/reference#adapter-get-relation) - added a `database` argument
  - [create_schema](https://docs.getdbt.com/v0.13/reference#adapter-create-schema) - added a `database` argument
  - [drop_schema](https://docs.getdbt.com/v0.13/reference#adapter-drop-schema) - added a `database` argument

### Deprecations
- The following adapter methods are now deprecated, and will be removed in a future release:
  - get_columns_in_table - deprecated in favor of [get_columns_in_relation](https://docs.getdbt.com/v0.13/reference#adapter-get-columns-in-relation)
  - already_exists - deprecated in favor of [get_relation](https://docs.getdbt.com/v0.13/reference#adapter-get-relation)

### Features
- Add `source`s to dbt, use them to calculate source data freshness ([docs](https://docs.getdbt.com/v0.13/docs/using-sources) ) ([#814](https://github.com/fishtown-analytics/dbt/issues/814), [#1240](https://github.com/fishtown-analytics/dbt/issues/1240))
- Add support for Presto ([docs](https://docs.getdbt.com/v0.13/docs/profile-presto), [repo](https://github.com/fishtown-analytics/dbt-presto)) ([#1106](https://github.com/fishtown-analytics/dbt/issues/1106))
- Add `require-dbt-version` option to `dbt_project.yml` to state the supported versions of dbt for packages ([docs](https://docs.getdbt.com/v0.13/docs/requiring-dbt-versions)) ([#581](https://github.com/fishtown-analytics/dbt/issues/581))
- Add an output line indicating the installed version of dbt to every run ([#1134](https://github.com/fishtown-analytics/dbt/issues/1134))
- Add a new model selector (`@`) which build models, their children, and their children's parents ([docs](https://docs.getdbt.com/v0.13/reference#section-the-at-operator)) ([#1156](https://github.com/fishtown-analytics/dbt/issues/1156))
- Add support for Snowflake Key Pair Authentication ([docs](https://docs.getdbt.com/v0.13/docs/profile-snowflake#section-key-pair-authentication)) ([#1232](https://github.com/fishtown-analytics/dbt/pull/1232))
- Support SSO Authentication for Snowflake ([docs](https://docs.getdbt.com/v0.13/docs/profile-snowflake#section-sso-authentication)) ([#1172](https://github.com/fishtown-analytics/dbt/issues/1172))
- Add support for Snowflake's transient tables ([docs](https://docs.getdbt.com/v0.13/docs/snowflake-configs#section-transient-tables)) ([#946](https://github.com/fishtown-analytics/dbt/issues/946))
- Capture build timing data in `run_results.json` to visualize project performance ([#1179](https://github.com/fishtown-analytics/dbt/issues/1179))
- Add CLI flag to toggle warnings as errors ([docs](https://docs.getdbt.com/v0.13/reference#section-treat-warnings-as-errors)) ([#1243](https://github.com/fishtown-analytics/dbt/issues/1243))
- Add tab completion script for Bash ([docs](https://github.com/fishtown-analytics/dbt-completion.bash)) ([#1197](https://github.com/fishtown-analytics/dbt/issues/1197))
- Added docs on how to build a new adapter ([docs](https://docs.getdbt.com/v0.13/docs/building-a-new-adapter)) ([#560](https://github.com/fishtown-analytics/dbt/issues/560))
- Use new logo ([#1349](https://github.com/fishtown-analytics/dbt/pull/1349))

### Fixes
- Fix for Postgres character columns treated as string types ([#1194](https://github.com/fishtown-analytics/dbt/issues/1194))
- Fix for hard to reach edge case in which dbt could hang ([#1223](https://github.com/fishtown-analytics/dbt/issues/1223))
- Fix for `dbt deps` in non-English shells ([#1222](https://github.com/fishtown-analytics/dbt/issues/1222))
- Fix for over eager schema creation when models are run with `--models` ([#1239](https://github.com/fishtown-analytics/dbt/issues/1239))
- Fix for `dbt seed --show` ([#1288](https://github.com/fishtown-analytics/dbt/issues/1288))
- Fix for `is_incremental()` which should only return `True` if the target relation is a `table` ([#1292](https://github.com/fishtown-analytics/dbt/issues/1292))
- Fix for error in Snowflake table materializations with custom schemas ([#1316](https://github.com/fishtown-analytics/dbt/issues/1316))
- Fix errored out concurrent transactions on Redshift and Postgres ([#1356](https://github.com/fishtown-analytics/dbt/pull/1356))
- Fix out of order execution on model select ([#1354](https://github.com/fishtown-analytics/dbt/issues/1354), [#1355](https://github.com/fishtown-analytics/dbt/pull/1355))
- Fix adapter macro namespace issue ([#1352](https://github.com/fishtown-analytics/dbt/issues/1352), [#1353](https://github.com/fishtown-analytics/dbt/pull/1353))
- Re-add CLI flag to toggle warnings as errors ([#1347](https://github.com/fishtown-analytics/dbt/pull/1347))
- Fix release candidate regression that runs run hooks on test invocations ([#1346](https://github.com/fishtown-analytics/dbt/pull/1346))
- Fix Snowflake source quoting ([#1338](https://github.com/fishtown-analytics/dbt/pull/1338), [#1317](https://github.com/fishtown-analytics/dbt/issues/1317), [#1332](https://github.com/fishtown-analytics/dbt/issues/1332))
- Handle unexpected max_loaded_at types ([#1330](https://github.com/fishtown-analytics/dbt/pull/1330))

### Under the hood
- Replace all SQL in Python code with Jinja in macros ([#1204](https://github.com/fishtown-analytics/dbt/issues/1204))
- Loosen restrictions of boto3 dependency ([#1234](https://github.com/fishtown-analytics/dbt/issues/1234))
- Rewrote Postgres introspective queries to be faster on large databases ([#1192](https://github.com/fishtown-analytics/dbt/issues/1192)


### Contributors:
Thanks for your contributions to dbt!

- [@patrickgoss](https://github.com/patrickgoss) [#1193](https://github.com/fishtown-analytics/dbt/issues/1193)
- [@brianhartsock](https://github.com/brianhartsock) [#1191](https://github.com/fishtown-analytics/dbt/pull/1191)
- [@alexyer](https://github.com/alexyer) [#1232](https://github.com/fishtown-analytics/dbt/pull/1232)
- [@adriank-convoy](https://github.com/adriank-convoy) [#1224](https://github.com/fishtown-analytics/dbt/pull/1224)
- [@mikekaminsky](https://github.com/mikekaminsky) [#1216](https://github.com/fishtown-analytics/dbt/pull/1216)
- [@vijaykiran](https://github.com/vijaykiran) [#1198](https://github.com/fishtown-analytics/dbt/pull/1198), [#1199](https://github.com/fishtown-analytics/dbt/pull/1199)

## dbt 0.12.2 - Grace Kelly (January 8, 2019)

### Overview

This release reduces the runtime of dbt projects by improving dbt's approach to model running. Additionally, a number of workflow improvements have been added.

### Deprecations
- Deprecate `sql_where` ([#744](https://github.com/fishtown-analytics/dbt/issues/744)) ([docs](https://docs.getdbt.com/v0.12/docs/configuring-incremental-models))

### Features
- More intelligently order and execute nodes in the graph. This _significantly_ speeds up the runtime of most dbt projects ([#813](https://github.com/fishtown-analytics/dbt/issues/813))
- Add `-m` flag as an alias for `--models` ([#1160](https://github.com/fishtown-analytics/dbt/issues/1160))
- Add `post_hook` and `pre_hook` as aliases for `post-hook` and `pre-hook`, respectively ([#1124](https://github.com/fishtown-analytics/dbt/issues/1124)) ([docs](https://docs.getdbt.com/v0.12/docs/using-hooks))
- Better handling of git errors in `dbt deps` + full support for Windows ([#994](https://github.com/fishtown-analytics/dbt/issues/994), [#778](https://github.com/fishtown-analytics/dbt/issues/778), [#895](https://github.com/fishtown-analytics/dbt/issues/895))
- Add support for specifying a `location` in BigQuery datasets ([#969](https://github.com/fishtown-analytics/dbt/issues/969)) ([docs](https://docs.getdbt.com/v0.12/docs/supported-databases#section-dataset-locations))
- Add support for Jinja expressions using the `{% do ... %}` block ([#1113](https://github.com/fishtown-analytics/dbt/issues/1113))
- The `dbt debug` command is actually useful now ([#1061](https://github.com/fishtown-analytics/dbt/issues/1061))
- The `config` function can now be called multiple times in a model ([#558](https://github.com/fishtown-analytics/dbt/issues/558))
- Source the latest version of dbt from PyPi instead of GitHub ([#1122](https://github.com/fishtown-analytics/dbt/issues/1122))
- Add a peformance profiling mechnanism to dbt ([#1001](https://github.com/fishtown-analytics/dbt/issues/1001))
- Add caching for dbt's macros-only manifest to speedup parsing ([#1098](https://github.com/fishtown-analytics/dbt/issues/1098))

### Fixes
- Fix for custom schemas used alongside the `generate_schema_name` macro ([#801](https://github.com/fishtown-analytics/dbt/issues/801))
- Fix for silent failure of tests that reference nonexistent models ([#968](https://github.com/fishtown-analytics/dbt/issues/968))
- Fix for `generate_schema_name` macros that return whitespace-padded schema names ([#1074](https://github.com/fishtown-analytics/dbt/issues/1074))
- Fix for incorrect relation type for backup tables on Snowflake ([#1103](https://github.com/fishtown-analytics/dbt/issues/1103))
- Fix for incorrectly cased values in the relation cache ([#1140](https://github.com/fishtown-analytics/dbt/issues/1140))
- Fix for JSON decoding error on Python2 installed with Anaconda ([#1155](https://github.com/fishtown-analytics/dbt/issues/1155))
- Fix for unhandled exceptions that occur in anonymous event tracking ([#1180](https://github.com/fishtown-analytics/dbt/issues/1180))
- Fix for analysis files that contain `raw` tags ([#1152](https://github.com/fishtown-analytics/dbt/issues/1152))
- Fix for packages which reference the [hubsite](hub.getdbt.com) ([#1095](https://github.com/fishtown-analytics/dbt/issues/1095))

## dbt 0.12.1 - (November 15, 2018)

### Overview

This is a bugfix release.

### Fixes

- Fix for relation caching when views outside of a dbt schema depend on relations inside of a dbt schema ([#1119](https://github.com/fishtown-analytics/dbt/issues/1119))


## dbt 0.12.0 - Guion Bluford (November 12, 2018)

### Overview

This release adds caching for some introspective queries on all adapters. Additionally, custom tags can be supplied for models, along with many other minor improvements and bugfixes.

### Breaking Changes
- Support for the `repositories:` block in `dbt_project.yml` (deprecated in 0.10.0) was removed.

### tl;dr
- Make runs faster by caching introspective queries
- Support [model tags](https://docs.getdbt.com/v0.12/docs/tags)
- Add a list of [schemas](https://docs.getdbt.com/v0.12/reference#schemas) to the `on-run-end` context
- Set your [profiles directory](https://docs.getdbt.com/v0.12/docs/configure-your-profile#section-using-the-dbt_profiles_dir-environment-variable) with an environment variable

### Features

- Cache the existence of relations to speed up dbt runs ([#1025](https://github.com/fishtown-analytics/dbt/pull/1025))
- Add support for tag configuration and selection ([#1014](https://github.com/fishtown-analytics/dbt/pull/1014))
  - Add tags to the model and graph views in the docs UI ([#7](https://github.com/fishtown-analytics/dbt-docs/pull/7))
- Add the set of schemas that dbt built models into in the `on-run-end` hook context ([#908](https://github.com/fishtown-analytics/dbt/issues/908))
- Warn for unused resource config paths in dbt_project.yml ([#725](https://github.com/fishtown-analytics/dbt/pull/725))
- Add more information to the `dbt --help` output ([#1058](https://github.com/fishtown-analytics/dbt/issues/1058))
- Add support for configuring the profiles directory with an env var ([#1055](https://github.com/fishtown-analytics/dbt/issues/1055))
- Add support for cli and env vars in most `dbt_project.yml` and `profiles.yml` fields ([#1033](https://github.com/fishtown-analytics/dbt/pull/1033))
- Provide a better error message when seed file loading fails on BigQuery ([#1079](https://github.com/fishtown-analytics/dbt/pull/1079))
- Improved error handling and messaging on Redshift ([#997](https://github.com/fishtown-analytics/dbt/issues/997))
- Include datasets with underscores when listing BigQuery datasets ([#954](https://github.com/fishtown-analytics/dbt/pull/954))
- Forgo validating the user's profile for `dbt deps` and `dbt clean` commands ([#947](https://github.com/fishtown-analytics/dbt/issues/947), [#1022](https://github.com/fishtown-analytics/dbt/issues/1022))
- Don't read/parse CSV files outside of the `dbt seed` command ([#1046](https://github.com/fishtown-analytics/dbt/pull/1046))

### Fixes

- Fix for incorrect model selection with the `--models` CLI flag when projects and directories share the same name ([#1023](https://github.com/fishtown-analytics/dbt/issues/1023))
- Fix for table clustering configuration with multiple columns on BigQuery ([#1013](https://github.com/fishtown-analytics/dbt/issues/1013))
- Fix for incorrect output when a single row fails validation in `dbt test` ([#1040](https://github.com/fishtown-analytics/dbt/issues/1040))
- Fix for unwieldly Jinja errors regarding undefined variables at parse time ([#1086](https://github.com/fishtown-analytics/dbt/pull/1086), [#1080](https://github.com/fishtown-analytics/dbt/issues/1080), [#935](https://github.com/fishtown-analytics/dbt/issues/935))
- Fix for incremental models that have a line comment on the last line of the file ([#1018](https://github.com/fishtown-analytics/dbt/issues/1018))
- Fix for error messages when ephemeral models fail to compile ([#1053](https://github.com/fishtown-analytics/dbt/pull/1053))


### Under the hood
- Create adapters as singleton objects instead of classes ([#961](https://github.com/fishtown-analytics/dbt/issues/961))
- Combine project and profile into a single, coherent object ([#973](https://github.com/fishtown-analytics/dbt/pull/973))
- Investigate approaches for providing more complete compilation output ([#588](https://github.com/fishtown-analytics/dbt/issues/588))


### Contributors

Thanks for contributing!

- [@mikekaminsky](https://github.com/mikekaminsky) ([#1049](https://github.com/fishtown-analytics/dbt/pull/1049), [#1060](https://github.com/fishtown-analytics/dbt/pull/1060))
- [@joshtemple](https://github.com/joshtemple) ([#1079](https://github.com/fishtown-analytics/dbt/pull/1079))
- [@k4y3ff](https://github.com/k4y3ff) ([#954](https://github.com/fishtown-analytics/dbt/pull/954))
- [@elexisvenator](https://github.com/elexisvenator) ([#1019](https://github.com/fishtown-analytics/dbt/pull/1019))
- [@clrcrl](https://github.com/clrcrl) ([#725](https://github.com/fishtown-analytics/dbt/pull/725)


## dbt 0.11.1 - Lucretia Mott (September 18, 2018)

### Overview

This is a patch release containing a few bugfixes and one quality of life change for dbt docs.

### Features

- dbt
  - Add `--port` parameter to dbt docs serve ([#987](https://github.com/fishtown-analytics/dbt/pull/987))

### Fixes

- dbt
  - Fix hooks in model configs not running ([#985](https://github.com/fishtown-analytics/dbt/pull/985))
  - Fix integration test on redshift catalog generation ([#977](https://github.com/fishtown-analytics/dbt/pull/977))
  - Snowflake: Fix docs generation errors when QUOTED_IDENTIFIER_IGNORE_CASE is set ([#998](https://github.com/fishtown-analytics/dbt/pull/998))
  - Translate empty strings to null in seeds ([#995](https://github.com/fishtown-analytics/dbt/pull/995))
  - Filter out null schemas during catalog generation ([#992](https://github.com/fishtown-analytics/dbt/pull/992))
  - Fix quoting on drop, truncate, and rename ([#991](https://github.com/fishtown-analytics/dbt/pull/991))
- dbt-docs
  - Fix for non-existent column in schema.yml ([#3](https://github.com/fishtown-analytics/dbt-docs/pull/3))
  - Fixes for missing tests in docs UI when columns are upcased ([#2](https://github.com/fishtown-analytics/dbt-docs/pull/2))
  - Fix "copy to clipboard" ([#4](https://github.com/fishtown-analytics/dbt-docs/issues/4))

## dbt 0.11.0 - Isaac Asimov (September 6, 2018)

### Overview

This release adds support for auto-generated dbt documentation, adds a new syntax for `schema.yml` files, and fixes a number of minor bugs. With the exception of planned changes to Snowflake's default quoting strategy, this release should not contain any breaking changes. Check out the [blog post](https://blog.fishtownanalytics.com/using-dbt-docs-fae6137da3c3) for more information about this release.

### Breaking Changes
- Change default Snowflake quoting strategy to "unquoted" ([docs](https://docs.getdbt.com/v0.11/docs/configuring-quoting)) ([#824](https://github.com/fishtown-analytics/dbt/issues/824))

### Features

- Add autogenerated dbt project documentation ([docs](https://docs.getdbt.com/v0.11/docs/testing-and-documentation)) ([#375](https://github.com/fishtown-analytics/dbt/issues/375), [#863](https://github.com/fishtown-analytics/dbt/issues/863), [#941](https://github.com/fishtown-analytics/dbt/issues/941), [#815](https://github.com/fishtown-analytics/dbt/issues/815))
- Version 2 of schema.yml, which allows users to create table and column comments that end up in the manifest ([docs](https://docs.getdbt.com/v0.11/docs/schemayml-files)) ([#880](https://github.com/fishtown-analytics/dbt/pull/880))
- Extend catalog and manifest to also support Snowflake, BigQuery, and Redshift, in addition to existing Postgres support ([#866](https://github.com/fishtown-analytics/dbt/pull/866), [#857](https://github.com/fishtown-analytics/dbt/pull/857), [#849](https://github.com/fishtown-analytics/dbt/pull/849))
- Add a 'generated_at' field to both the manifest and the catalog. ([#887](https://github.com/fishtown-analytics/dbt/pull/877))
- Add `docs` blocks that users can put into `.md` files and `doc()` value for schema v2 description fields ([#888](https://github.com/fishtown-analytics/dbt/pull/888))
- Write out a 'run_results.json' after dbt invocations. ([#904](https://github.com/fishtown-analytics/dbt/pull/904))
- Type inference for interpreting CSV data is now less aggressive ([#905](https://github.com/fishtown-analytics/dbt/pull/905))
- Remove distinction between `this.table` and `this.schema` by refactoring materialization SQL ([#940](https://github.com/fishtown-analytics/dbt/pull/940))

### Fixes
- Fix for identifier clashes in BigQuery merge statements ([#914](https://github.com/fishtown-analytics/dbt/issues/914))
- Fix for unneccessary downloads of `bumpversion.cfg`, handle failures gracefully ([#907](https://github.com/fishtown-analytics/dbt/issues/907))
- Fix for incompatible `boto3` requirements ([#959](https://github.com/fishtown-analytics/dbt/issues/959))
- Fix for invalid `relationships` test when the parent column contains null values ([#921](https://github.com/fishtown-analytics/dbt/pull/921))

### Contributors

Thanks for contributing!

- [@rsmichaeldunn](https://github.com/rsmichaeldunn) ([#799](https://github.com/fishtown-analytics/dbt/pull/799))
- [@lewish](https://github.com/fishtown-analytics/dbt/pull/915) ([#915](https://github.com/fishtown-analytics/dbt/pull/915))
- [@MartinLue](https://github.com/MartinLue) ([#872](https://github.com/fishtown-analytics/dbt/pull/872))

## dbt 0.10.2 - Betsy Ross (August 3, 2018)

### Overview

This release makes it possible to alias relation names, rounds out support for BigQuery with incremental, archival, and hook support, adds the IAM Auth method for Redshift, and builds the foundation for autogenerated dbt project documentation, to come in the next release.

Additionally, a number of bugs have been fixed including intermittent BigQuery 404 errors, Redshift "table dropped by concurrent query" errors, and a probable fix for Redshift connection timeout issues.

### Contributors

We want to extend a big thank you to our outside contributors for this release! You all are amazing.

- [@danielchalef](https://github.com/danielchalef) ([#818](https://github.com/fishtown-analytics/dbt/pull/818))
- [@mjumbewu](https://github.com/mjumbewu) ([#796](https://github.com/fishtown-analytics/dbt/pull/796))
- [@abelsonlive](https://github.com/abelsonlive) ([#800](https://github.com/fishtown-analytics/dbt/pull/800))
- [@jon-rtr](https://github.com/jon-rtr) ([#800](https://github.com/fishtown-analytics/dbt/pull/800))
- [@mturzanska](https://github.com/mturzanska) ([#797](https://github.com/fishtown-analytics/dbt/pull/797))
- [@cpdean](https://github.com/cpdean) ([#780](https://github.com/fishtown-analytics/dbt/pull/780))

### Features

- BigQuery
  - Support incremental models ([#856](https://github.com/fishtown-analytics/dbt/pull/856)) ([docs](https://docs.getdbt.com/docs/configuring-models#section-configuring-incremental-models))
  - Support archival ([#856](https://github.com/fishtown-analytics/dbt/pull/856)) ([docs](https://docs.getdbt.com/docs/archival))
  - Add pre/post hook support ([#836](https://github.com/fishtown-analytics/dbt/pull/836)) ([docs](https://docs.getdbt.com/docs/using-hooks))
- Redshift: IAM Auth ([#818](https://github.com/fishtown-analytics/dbt/pull/818)) ([docs](https://docs.getdbt.com/docs/supported-databases#section-iam-authentication))
- Model aliases ([#800](https://github.com/fishtown-analytics/dbt/pull/800))([docs](https://docs.getdbt.com/docs/using-custom-aliases))
- Write JSON manifest file to disk during compilation ([#761](https://github.com/fishtown-analytics/dbt/pull/761))
- Add forward and backward graph edges to the JSON manifest file ([#762](https://github.com/fishtown-analytics/dbt/pull/762))
- Add a 'dbt docs generate' command to generate a JSON catalog file ([#774](https://github.com/fishtown-analytics/dbt/pull/774), [#808](https://github.com/fishtown-analytics/dbt/pull/808))

### Bugfixes

- BigQuery: fix concurrent relation loads ([#835](https://github.com/fishtown-analytics/dbt/pull/835))
- BigQuery: support external relations ([#828](https://github.com/fishtown-analytics/dbt/pull/828))
- Redshift: set TCP keepalive on connections ([#826](https://github.com/fishtown-analytics/dbt/pull/826))
- Redshift: fix "table dropped by concurrent query" ([#825](https://github.com/fishtown-analytics/dbt/pull/825))
- Fix the error handling for profiles.yml validation ([#820](https://github.com/fishtown-analytics/dbt/pull/820))
- Make the `--threads` parameter actually change the number of threads used ([#819](https://github.com/fishtown-analytics/dbt/pull/819))
- Ensure that numeric precision of a column is not `None` ([#796](https://github.com/fishtown-analytics/dbt/pull/796))
- Allow for more complex version comparison ([#797](https://github.com/fishtown-analytics/dbt/pull/797))

### Changes

- Use a subselect instead of CTE when building incremental models ([#787](https://github.com/fishtown-analytics/dbt/pull/787))
- Internals
  - Improved dependency selection, rip out some unused dependencies ([#848](https://github.com/fishtown-analytics/dbt/pull/848))
  - Stop tracking `run_error` in tracking code ([#817](https://github.com/fishtown-analytics/dbt/pull/817))
  - Use Mapping instead of dict as the base class for APIObject ([#756](https://github.com/fishtown-analytics/dbt/pull/756))
  - Split out parsers ([#809](https://github.com/fishtown-analytics/dbt/pull/809))
  - Fix `__all__` parameter in submodules ([#780](https://github.com/fishtown-analytics/dbt/pull/780))
  - Switch to CircleCI 2.0 ([#843](https://github.com/fishtown-analytics/dbt/pull/843), [#850](https://github.com/fishtown-analytics/dbt/pull/850))
  - Added tox environments that have the user specify what tests should be run ([#837](https://github.com/fishtown-analytics/dbt/pull/837))

## dbt 0.10.1 (May 18, 2018)

This release focuses on achieving functional parity between all of dbt's adapters. With this release, most dbt functionality should work on every adapter except where noted [here](https://docs.getdbt.com/v0.10/docs/supported-databases#section-caveats).

### tl;dr
 - Configure model schema and name quoting in your `dbt_project.yml` file ([Docs](https://docs.getdbt.com/v0.10/docs/configuring-quoting))
 - Add a `Relation` object to the context to simplify model quoting [Docs](https://docs.getdbt.com/v0.10/reference#relation)
 - Implement BigQuery materializations using new `create table as (...)` syntax, support `partition by` clause ([Docs](https://docs.getdbt.com/v0.10/docs/warehouse-specific-configurations#section-partition-clause))
 - Override seed column types ([Docs](https://docs.getdbt.com/v0.10/reference#section-override-column-types))
 - Add `get_columns_in_table` context function for BigQuery ([Docs](https://docs.getdbt.com/v0.10/reference#get_columns_in_table))

### Changes
 - Consistent schema and identifier quoting ([#727](https://github.com/fishtown-analytics/dbt/pull/727))
   - Configure quoting settings in the `dbt_project.yml` file ([#742](https://github.com/fishtown-analytics/dbt/pull/742))
   - Add a `Relation` object to the context to make quoting consistent and simple ([#742](https://github.com/fishtown-analytics/dbt/pull/742))
 - Use the new `create table as (...)` syntax on BigQuery ([#717](https://github.com/fishtown-analytics/dbt/pull/717))
   - Support `partition by` clause
 - CSV Updates:
   - Use floating point as default seed column type to avoid issues with type inference ([#694](https://github.com/fishtown-analytics/dbt/pull/694))
   - Provide a mechanism for overriding seed column types in the `dbt_project.yml` file ([#708](https://github.com/fishtown-analytics/dbt/pull/708))
   - Fix seeding for files with more than 16k rows on Snowflake ([#694](https://github.com/fishtown-analytics/dbt/pull/694))
   - Implement seeds using a materialization
 - Improve `get_columns_in_table` context function ([#709](https://github.com/fishtown-analytics/dbt/pull/709))
   - Support numeric types on Redshift, Postgres
   - Support BigQuery (including nested columns in `struct` types)
   - Support cross-database `information_schema` queries for Snowflake
   - Retain column ordinal positions

### Bugfixes
 - Fix for incorrect var precendence when using `--vars` on the CLI ([#739](https://github.com/fishtown-analytics/dbt/pull/739))
 - Fix for closed connections in `on-run-end` hooks for long-running dbt invocations ([#693](https://github.com/fishtown-analytics/dbt/pull/693))
 - Fix: don't try to run empty hooks ([#620](https://github.com/fishtown-analytics/dbt/issues/620), [#693](https://github.com/fishtown-analytics/dbt/pull/693))
 - Fix: Prevent seed data from being serialized into `graph.gpickle` file ([#720](https://github.com/fishtown-analytics/dbt/pull/720))
 - Fix: Disallow seed and model files with the same name ([#737](https://github.com/fishtown-analytics/dbt/pull/737))

## dbt 0.10.0 (March 8, 2018)

This release overhauls dbt's package management functionality, makes seeding csv files work across all adapters, and adds date partitioning support for BigQuery.

### Upgrading Instructions:
 - Check out full installation and upgrading instructions [here](https://docs.getdbt.com/docs/installation)
 - Transition the `repositories:` section of your `dbt_project.yml` file to a `packages.yml` file as described [here](https://docs.getdbt.com/docs/package-management)
 - You may need to clear out your `dbt_modules` directory if you use packages like [dbt-utils](https://github.com/fishtown-analytics/dbt-utils). Depending how your project is configured, you can do this by running `dbt clean`.
 - We're using a new CSV parsing library, `agate`, so be sure to check that all of your seed tables are parsed as you would expect!


### Changes
- Support for variables defined on the CLI with `--vars` ([#640](https://github.com/fishtown-analytics/dbt/pull/640)) ([docs](https://docs.getdbt.com/docs/using-variables))
- Improvements to `dbt seed` ([docs](https://docs.getdbt.com/v0.10/reference#seed))
  - Support seeding csv files on all adapters ([#618](https://github.com/fishtown-analytics/dbt/pull/618))
  - Make seed csv's `ref()`-able in models ([#668](https://github.com/fishtown-analytics/dbt/pull/668))
  - Support seed file configuration (custom schemas, enabled / disabled) in the `dbt_project.yml` file ([#561](https://github.com/fishtown-analytics/dbt/issues/561))
  - Support `--full-refresh` instead of `--drop-existing` (deprecated) for seed files ([#515](https://github.com/fishtown-analytics/dbt/issues/515))
  - Add `--show` argument to `dbt seed` to display a sample of data in the CLI ([#74](https://github.com/fishtown-analytics/dbt/issues/74))
- Improvements to package management ([docs](https://docs.getdbt.com/docs/package-management))
  - Deprecated `repositories:` config option in favor of `packages:` ([#542](https://github.com/fishtown-analytics/dbt/pull/542))
  - Deprecated package listing in `dbt_project.yml` in favor of `packages.yml` ([#681](https://github.com/fishtown-analytics/dbt/pull/681))
  - Support stating local file paths as dependencies ([#542](https://github.com/fishtown-analytics/dbt/pull/542))
- Support date partitioning in BigQuery ([#641](https://github.com/fishtown-analytics/dbt/pull/641)) ([docs](https://docs.getdbt.com/docs/creating-date-partitioned-tables))
- Move schema creation to _after_ `on-run-start` hooks ([#652](https://github.com/fishtown-analytics/dbt/pull/652))
- Replace `csvkit` dependency with `agate` ([#598](https://github.com/fishtown-analytics/dbt/issues/598))
- Switch snowplow endpoint to pipe directly to Fishtown Analytics ([#682](https://github.com/fishtown-analytics/dbt/pull/682))

### Bugfixes
- Throw a compilation exception if a required test macro is not present in the context ([#655](https://github.com/fishtown-analytics/dbt/issues/655))
- Make the `adapter_macro` use the `return()` function ([#635](https://github.com/fishtown-analytics/dbt/issues/635))
- Fix bug for introspective query on late binding views (redshift) ([#647](https://github.com/fishtown-analytics/dbt/pull/647))
- Disable any non-dbt log output on the CLI ([#663](https://github.com/fishtown-analytics/dbt/pull/663))


## dbt 0.9.1 (January 2, 2018)

This release fixes bugs and adds supports for late binding views on Redshift.

### Changes
- Support late binding views on Redshift ([#614](https://github.com/fishtown-analytics/dbt/pull/614)) ([docs](https://docs.getdbt.com/docs/warehouse-specific-configurations#section-late-binding-views))
- Make `run_started_at` timezone-aware ([#553](https://github.com/fishtown-analytics/dbt/pull/553)) (Contributed by [@mturzanska](https://github.com/mturzanska)) ([docs](https://docs.getdbt.com/v0.9/reference#run_started_at))

### Bugfixes

- Include hook run time in reported model run time ([#607](https://github.com/fishtown-analytics/dbt/pull/607))
- Add warning for missing test constraints ([#600](https://github.com/fishtown-analytics/dbt/pull/600))
- Fix for schema tests used or defined in packages ([#599](https://github.com/fishtown-analytics/dbt/pull/599))
- Run hooks in defined order ([#601](https://github.com/fishtown-analytics/dbt/pull/601))
- Skip tests that depend on nonexistent models ([#617](https://github.com/fishtown-analytics/dbt/pull/617))
- Fix for `adapter_macro` called within a package ([#630](https://github.com/fishtown-analytics/dbt/pull/630))


## dbt 0.9.0 (October 25, 2017)

This release focuses on improvements to macros, materializations, and package management. Check out [the blog post](https://blog.fishtownanalytics.com/whats-new-in-dbt-0-9-0-dd36f3572ac6) to learn more about what's possible in this new version of dbt.

### Installation

Full installation instructions for macOS, Windows, and Linux can be found [here](https://docs.getdbt.com/v0.9/docs/installation). If you use Windows or Linux, installation works the same as with previous versions of dbt. If you use macOS and Homebrew to install dbt, note that installation instructions have changed:

#### macOS Installation Instructions
```bash
brew update
brew tap fishtown-analytics/dbt
brew install dbt
```

### Overview

- More powerful macros and materializations
- Custom model schemas
- BigQuery improvements
- Bugfixes
- Documentation (0.9.0 docs can be found [here](https://docs.getdbt.com/v0.9/))


### Breaking Changes
- `adapter` functions must be namespaced to the `adapter` context variable. To fix this error, use `adapter.already_exists` instead of just `already_exists`, or similar for other [adapter functions](https://docs.getdbt.com/docs/adapter).


### Bugfixes
- Handle lingering `__dbt_tmp` relations ([#511](https://github.com/fishtown-analytics/dbt/pull/511))
- Run tests defined in an ephemeral directory ([#509](https://github.com/fishtown-analytics/dbt/pull/509))


### Changes
- use `adapter`, `ref`, and `var` inside of macros ([#466](https://github.com/fishtown-analytics/dbt/pull/466/files))
- Build custom tests and materializations in dbt packages ([#466](https://github.com/fishtown-analytics/dbt/pull/466/files))
- Support pre- and post- hooks that run outside of a transaction ([#510](https://github.com/fishtown-analytics/dbt/pull/510))
- Support table materializations for BigQuery ([#507](https://github.com/fishtown-analytics/dbt/pull/507))
- Support querying external data sources in BigQuery ([#507](https://github.com/fishtown-analytics/dbt/pull/507))
- Override which schema models are materialized in ([#522](https://github.com/fishtown-analytics/dbt/pull/522)) ([docs](https://docs.getdbt.com/v0.9/docs/using-custom-schemas))
- Make `{{ ref(...) }}` return the same type of object as `{{ this }} `([#530](https://github.com/fishtown-analytics/dbt/pull/530))
- Replace schema test CTEs with subqueries to speed them up for Postgres ([#536](https://github.com/fishtown-analytics/dbt/pull/536)) ([@ronnyli](https://github.com/ronnyli))
 - Bump Snowflake dependency, remove pyasn1 ([#570](https://github.com/fishtown-analytics/dbt/pull/570))


### Documentation
- Document how to [create a package](https://docs.getdbt.com/v0.9/docs/building-packages)
- Document how to [make a materialization](https://docs.getdbt.com/v0.9/docs/creating-new-materializations)
- Document how to [make custom schema tests](https://docs.getdbt.com/v0.9/docs/custom-schema-tests)
- Document how to [use hooks to vacuum](https://docs.getdbt.com/v0.9/docs/using-hooks#section-using-hooks-to-vacuum)
- Document [all context variables](https://docs.getdbt.com/v0.9/reference)


### New Contributors
- [@ronnyli](https://github.com/ronnyli) ([#536](https://github.com/fishtown-analytics/dbt/pull/536))


## dbt 0.9.0 Alpha 5 (October 24, 2017)

### Overview
 - Bump Snowflake dependency, remove pyasn1 ([#570](https://github.com/fishtown-analytics/dbt/pull/570))

## dbt 0.9.0 Alpha 4 (October 3, 2017)

### Bugfixes
 - Fix for federated queries on BigQuery with Service Account json credentials ([#547](https://github.com/fishtown-analytics/dbt/pull/547))

## dbt 0.9.0 Alpha 3 (October 3, 2017)

### Overview
 - Bugfixes
 - Faster schema tests on Postgres
 - Fix for broken environment variables

### Improvements

- Replace schema test CTEs with subqueries to speed them up for Postgres ([#536](https://github.com/fishtown-analytics/dbt/pull/536)) ([@ronnyli](https://github.com/ronnyli))

### Bugfixes
- Fix broken integration tests ([#539](https://github.com/fishtown-analytics/dbt/pull/539))
- Fix for `--non-destructive` on views ([#539](https://github.com/fishtown-analytics/dbt/pull/539))
- Fix for package models materialized in the wrong schema ([#538](https://github.com/fishtown-analytics/dbt/pull/538))
- Fix for broken environment variables ([#543](https://github.com/fishtown-analytics/dbt/pull/543))

### New Contributors

- [@ronnyli](https://github.com/ronnyli)
  - https://github.com/fishtown-analytics/dbt/pull/536

## dbt 0.9.0 Alpha 2 (September 20, 2017)

### Overview

- Custom model schemas
- BigQuery updates
- `ref` improvements

### Bugfixes
- Parity for `statement` interface on BigQuery ([#526](https://github.com/fishtown-analytics/dbt/pull/526))

### Changes
- Override which schema models are materialized in ([#522](https://github.com/fishtown-analytics/dbt/pull/522)) ([docs](https://docs.getdbt.com/v0.9/docs/using-custom-schemas))
- Make `{{ ref(...) }}` return the same type of object as `{{ this }} `([#530](https://github.com/fishtown-analytics/dbt/pull/530))


## dbt 0.9.0 Alpha 1 (August 29, 2017)

### Overview

- More powerful macros
- BigQuery improvements
- Bugfixes
- Documentation (0.9.0 docs can be found [here](https://docs.getdbt.com/v0.9/))

### Breaking Changes
dbt 0.9.0 Alpha 1 introduces a number of new features intended to help dbt-ers write flexible, reusable code. The majority of these changes involve the `macro` and `materialization` Jinja blocks. As this is an alpha release, there may exist bugs or incompatibilites, particularly surrounding these two blocks. A list of known breaking changes is provided below. If you find new bugs, or have questions about dbt 0.9.0, please don't hesitate to reach out in [slack](http://slack.getdbt.com/) or [open a new issue](https://github.com/fishtown-analytics/dbt/issues/new?milestone=0.9.0+alpha-1).

##### 1. Adapter functions must be namespaced to the `adapter` context variable
This will manifest as a compilation error that looks like:
```
Compilation Error in model {your_model} (models/path/to/your_model.sql)
  'already_exists' is undefined
```

To fix this error, use `adapter.already_exists` instead of just `already_exists`, or similar for other [adapter functions](https://docs.getdbt.com/docs/adapter).

### Bugfixes
- Handle lingering `__dbt_tmp` relations ([#511](https://github.com/fishtown-analytics/dbt/pull/511))
- Run tests defined in an ephemeral directory ([#509](https://github.com/fishtown-analytics/dbt/pull/509))

### Changes
- use `adapter`, `ref`, and `var` inside of macros ([#466](https://github.com/fishtown-analytics/dbt/pull/466/files))
- Build custom tests and materializations in dbt packages ([#466](https://github.com/fishtown-analytics/dbt/pull/466/files))
- Support pre- and post- hooks that run outside of a transaction ([#510](https://github.com/fishtown-analytics/dbt/pull/510))
- Support table materializations for BigQuery ([#507](https://github.com/fishtown-analytics/dbt/pull/507))
- Support querying external data sources in BigQuery ([#507](https://github.com/fishtown-analytics/dbt/pull/507))

### Documentation
- Document how to [create a package](https://docs.getdbt.com/v0.8/docs/building-packages)
- Document how to [make a materialization](https://docs.getdbt.com/v0.8/docs/creating-new-materializations)
- Document how to [make custom schema tests](https://docs.getdbt.com/v0.8/docs/custom-schema-tests)

## dbt 0.8.3 (July 14, 2017)

### Overview

- Add suppport for Google BigQuery
- Significant exit codes
- Load credentials from environment variables

### Bugfixes

- Fix errant warning for `dbt archive` commands ([#476](https://github.com/fishtown-analytics/dbt/pull/476))
- Show error (instead of backtrace) for failed hook statements ([#478](https://github.com/fishtown-analytics/dbt/pull/478))
- `dbt init` no longer leaves the repo in an invalid state ([#487](https://github.com/fishtown-analytics/dbt/pull/487))
- Fix bug which ignored git tag specs for package repos ([#463](https://github.com/fishtown-analytics/dbt/issues/463))

### Changes

- Support BigQuery as a target ([#437](https://github.com/fishtown-analytics/dbt/issues/437)) ([#438](https://github.com/fishtown-analytics/dbt/issues/438))
- Make dbt exit codes significant (0 = success, 1/2 = error) ([#297](https://github.com/fishtown-analytics/dbt/issues/297))
- Add context function to pull in environment variables ([#450](https://github.com/fishtown-analytics/dbt/issues/450))

### Documentation
- Document target configuration for BigQuery [here](https://docs.getdbt.com/v0.8/docs/supported-databases#section-bigquery)
- Document dbt exit codes [here](https://docs.getdbt.com/v0.8/reference#exit-codes)
- Document environment variable usage [here](https://docs.getdbt.com/v0.8/reference#env_var)

## dbt 0.8.2 (May 31, 2017)

### Overview

- UI/UX improvements (colorized output, failures summary, better error messages)
- Cancel running queries on ctrl+c
- Bugfixes
- Docs

### Bugfixes

- Fix bug for interleaved sort keys on Redshift ([#430](https://github.com/fishtown-analytics/dbt/pull/430))

### Changes
- Don't try to create schema if it already exists ([#446](https://github.com/fishtown-analytics/dbt/pull/446))
- Summarize failures for dbt invocations ([#443](https://github.com/fishtown-analytics/dbt/pull/443))
- Colorized dbt output ([#441](https://github.com/fishtown-analytics/dbt/pull/441))
- Cancel running queries on ctrl-c ([#444](https://github.com/fishtown-analytics/dbt/pull/444))
- Better error messages for common failure modes ([#445](https://github.com/fishtown-analytics/dbt/pull/445))
- Upgrade dependencies ([#431](https://github.com/fishtown-analytics/dbt/pull/431))
- Improvements to `dbt init` and first time dbt usage experience ([#439](https://github.com/fishtown-analytics/dbt/pull/439))

### Documentation
- Document full-refresh requirements for incremental models ([#417](https://github.com/fishtown-analytics/dbt/issues/417))
- Document archival ([#433](https://github.com/fishtown-analytics/dbt/issues/433))
- Document the two-version variant of `ref` ([#432](https://github.com/fishtown-analytics/dbt/issues/432))


## dbt 0.8.1 (May 10, 2017)


### Overview
- Bugfixes
- Reintroduce `compile` command
- Moved docs to [readme.io](https://docs.getdbt.com/)


### Bugfixes

- Fix bug preventing overriding a disabled package model in the current project ([#391](https://github.com/fishtown-analytics/dbt/pull/391))
- Fix bug which prevented multiple sort keys (provided as an array) on Redshift ([#397](https://github.com/fishtown-analytics/dbt/pull/397))
- Fix race condition while compiling schema tests in an empty `target` directory ([#398](https://github.com/fishtown-analytics/dbt/pull/398))

### Changes

- Reintroduce dbt `compile` command ([#407](https://github.com/fishtown-analytics/dbt/pull/407))
- Compile `on-run-start` and `on-run-end` hooks to a file ([#412](https://github.com/fishtown-analytics/dbt/pull/412))

### Documentation
- Move docs to readme.io ([#414](https://github.com/fishtown-analytics/dbt/pull/414))
- Add docs for event tracking opt-out ([#399](https://github.com/fishtown-analytics/dbt/issues/399))


## dbt 0.8.0 (April 17, 2017)


### Overview

- Bugfixes
- True concurrency
- More control over "advanced" incremental model configurations [more info](http://dbt.readthedocs.io/en/master/guide/configuring-models/)

### Bugfixes

- Fix ephemeral load order bug ([#292](https://github.com/fishtown-analytics/dbt/pull/292), [#285](https://github.com/fishtown-analytics/dbt/pull/285))
- Support composite unique key in archivals ([#324](https://github.com/fishtown-analytics/dbt/pull/324))
- Fix target paths ([#331](https://github.com/fishtown-analytics/dbt/pull/331), [#329](https://github.com/fishtown-analytics/dbt/issues/329))
- Ignore commented-out schema tests ([#330](https://github.com/fishtown-analytics/dbt/pull/330), [#328](https://github.com/fishtown-analytics/dbt/issues/328))
- Fix run levels ([#343](https://github.com/fishtown-analytics/dbt/pull/343), [#340](https://github.com/fishtown-analytics/dbt/issues/340), [#338](https://github.com/fishtown-analytics/dbt/issues/338))
- Fix concurrency, open a unique transaction per model ([#345](https://github.com/fishtown-analytics/dbt/pull/345), [#336](https://github.com/fishtown-analytics/dbt/issues/336))
- Handle concurrent `DROP ... CASCADE`s in Redshift ([#349](https://github.com/fishtown-analytics/dbt/pull/349))
- Always release connections (use `try .. finally`) ([#354](https://github.com/fishtown-analytics/dbt/pull/354))

### Changes

- Changed: different syntax for "relationships" schema tests ([#339](https://github.com/fishtown-analytics/dbt/pull/339))
- Added: `already_exists` context function ([#372](https://github.com/fishtown-analytics/dbt/pull/372))
- Graph refactor: fix common issues with load order ([#292](https://github.com/fishtown-analytics/dbt/pull/292))
- Graph refactor: multiple references to an ephemeral models should share a CTE ([#316](https://github.com/fishtown-analytics/dbt/pull/316))
- Graph refactor: macros in flat graph ([#332](https://github.com/fishtown-analytics/dbt/pull/332))
- Refactor: factor out jinja interactions ([#309](https://github.com/fishtown-analytics/dbt/pull/309))
- Speedup: detect cycles at the end of compilation ([#307](https://github.com/fishtown-analytics/dbt/pull/307))
- Speedup: write graph file with gpickle instead of yaml ([#306](https://github.com/fishtown-analytics/dbt/pull/306))
- Clone dependencies with `--depth 1` to make them more compact ([#277](https://github.com/fishtown-analytics/dbt/issues/277), [#342](https://github.com/fishtown-analytics/dbt/pull/342))
- Rewrite materializations as macros ([#356](https://github.com/fishtown-analytics/dbt/pull/356))

## dbt 0.7.1 (February 28, 2017)

### Overview

- [Improved graph selection](http://dbt.readthedocs.io/en/master/guide/usage/#run)
- A new home for dbt
- Snowflake improvements

#### New Features

- improved graph selection for `dbt run` and `dbt test` ([more information](http://dbt.readthedocs.io/en/master/guide/usage/#run)) ([#279](https://github.com/fishtown-analytics/dbt/pull/279))
- profiles.yml now supports Snowflake `role` as an option ([#291](https://github.com/fishtown-analytics/dbt/pull/291))

#### A new home for dbt

In v0.7.1, dbt was moved from the analyst-collective org to the fishtown-analytics org ([#300](https://github.com/fishtown-analytics/dbt/pull/300))

#### Bugfixes

- nicer error if `run-target` was not changed to `target` during upgrade to dbt>=0.7.0


## dbt 0.7.0 (February 9, 2017)

### Overview

- Snowflake Support
- Deprecations

### Snowflake Support

dbt now supports [Snowflake](https://www.snowflake.net/) as a target in addition to Postgres and Redshift! All dbt functionality is supported in this new warehouse. There is a sample snowflake profile in [sample.profiles.yml](https://github.com/fishtown-analytics/dbt/blob/development/sample.profiles.yml) -- you can start using it right away.

### Deprecations

There are a few deprecations in 0.7:

 - `run-target` in profiles.yml is no longer supported. Use `target` instead.
 - Project names (`name` in dbt_project.yml) can now only contain letters, numbers, and underscores, and must start with a letter. Previously they could contain any character.
 - `--dry-run` is no longer supported.

### Notes

#### New Features

- dbt now supports [Snowflake](https://www.snowflake.net/) as a warehouse ([#259](https://github.com/fishtown-analytics/dbt/pull/259))

#### Bugfixes

- use adapter for sort/dist ([#274](https://github.com/fishtown-analytics/dbt/pull/274))

#### Deprecations

- run-target and name validations ([#280](https://github.com/fishtown-analytics/dbt/pull/280))
- dry-run removed ([#281](https://github.com/fishtown-analytics/dbt/pull/281))

#### Changes

- fixed a typo in the docs related to post-run hooks ([#271](https://github.com/fishtown-analytics/dbt/pull/271))
- refactored tracking code to refresh invocation id in a multi-run context ([#273](https://github.com/fishtown-analytics/dbt/pull/273))
- added unit tests for the graph ([#270](https://github.com/fishtown-analytics/dbt/pull/270))

## dbt 0.6.2 (January 16, 2017)

#### Changes

- condense error output when `--debug` is not set ([#265](https://github.com/fishtown-analytics/dbt/pull/265))

## dbt 0.6.1 (January 11, 2017)

#### Bugfixes

- respect `config` options in profiles.yml ([#255](https://github.com/fishtown-analytics/dbt/pull/255))
- use correct `on-run-end` option for post-run hooks ([#261](https://github.com/fishtown-analytics/dbt/pull/261))

#### Changes

- add `--debug` flag, replace calls to `print()` with a global logger ([#256](https://github.com/fishtown-analytics/dbt/pull/256))
- add pep8 check to continuous integration tests and bring codebase into compliance ([#257](https://github.com/fishtown-analytics/dbt/pull/257))

## dbt release 0.6.0

### tl;dr
 - Macros
 - More control over how models are materialized
 - Minor improvements
 - Bugfixes
 - Connor McArthur

### Macros

Macros are snippets of SQL that can be called like functions in models. Macros make it possible to re-use SQL between models
in keeping with the engineering principle of DRY (Dont Repeat Yourself). Moreover, packages can expose Macros that you can use in your own dbt project.

For detailed information on how to use Macros, check out the pull request [here](https://github.com/fishtown-analytics/dbt/pull/245)


### Runtime Materialization Configs
DBT Version 0.6.0 introduces two new ways to control the materialization of models:

#### Non-destructive dbt run [more info](https://github.com/fishtown-analytics/dbt/issues/137)

If you provide the `--non-destructive` argument to `dbt run`, dbt will minimize the amount of time during which your models are unavailable. Specfically, dbt
will
 1. Ignore models materialized as `views`
 2. Truncate tables and re-insert data instead of dropping and re-creating

This flag is useful for recurring jobs which only need to update table models and incremental models.

```bash
dbt run --non-destructive
```

#### Incremental Model Full Refresh [more info](https://github.com/fishtown-analytics/dbt/issues/140)

If you provide the `--full-refresh` argument to `dbt run`, dbt will treat incremental models as table models. This is useful when

1. An incremental model schema changes and you need to recreate the table accordingly
2. You want to reprocess the entirety of the incremental model because of new logic in the model code

```bash
dbt run --full-refresh
```

Note that `--full-refresh` and `--non-destructive` can be used together!

For more information, run
```
dbt run --help
```

### Minor improvements [more info](https://github.com/fishtown-analytics/dbt/milestone/15?closed=1)

#### Add a `{{ target }}` variable to the dbt runtime [more info](https://github.com/fishtown-analytics/dbt/issues/149)
Use `{{ target }}` to interpolate profile variables into your model definitions. For example:

```sql
-- only use the last week of data in development
select * from events

{% if target.name == 'dev' %}
where created_at > getdate() - interval '1 week'
{% endif %}
```

#### User-specified `profiles.yml` dir [more info](https://github.com/fishtown-analytics/dbt/issues/213)
DBT looks for a file called `profiles.yml` in the `~/.dbt/` directory. You can now overide this directory with
```bash
$ dbt run --profiles-dir /path/to/my/dir
```
#### Add timestamp to console output [more info](https://github.com/fishtown-analytics/dbt/issues/125)
Informative _and_ pretty

#### Run dbt from subdirectory of project root [more info](https://github.com/fishtown-analytics/dbt/issues/129)
A story in three parts:
```bash
cd models/snowplow/sessions
vim sessions.sql
dbt run # it works!
```

#### Pre and post run hooks [more info](https://github.com/fishtown-analytics/dbt/issues/226)
```yaml
# dbt_project.yml
name: ...
version: ...

...

# supply either a string, or a list of strings
on-run-start: "create table public.cool_table (id int)"
on-run-end:
  - insert into public.cool_table (id) values (1), (2), (3)
  - insert into public.cool_table (id) values (4), (5), (6)
```

### Bug fixes

We fixed 10 bugs in this release! See the full list [here](https://github.com/fishtown-analytics/dbt/milestone/11?closed=1)

---

## dbt release 0.5.4

### tl;dr
- added support for custom SQL data tests
  - SQL returns 0 results --> pass
  - SQL returns > 0 results --> fail
- dbt-core integration tests
  - running in Continuous Integration environments
    - windows ([appveyor](https://ci.appveyor.com/project/DrewBanin/dbt/branch/development))
    - linux ([circle](https://circleci.com/gh/fishtown-analytics/dbt/tree/master))
  - with [code coverage](https://circleci.com/api/v1/project/fishtown-analytics/dbt/latest/artifacts/0/$CIRCLE_ARTIFACTS/htmlcov/index.html?branch=development)


### Custom SQL data tests

Schema tests have proven to be an essential part of a modern analytical workflow. These schema tests validate basic constraints about your data. Namely: not null, unique, accepted value, and foreign key relationship properties can be asserted using schema tests.

With dbt v0.5.4, you can now write your own custom "data tests". These data tests are SQL SELECT statements that return 0 rows on success, or > 0 rows on failure. A typical data test might look like:

```sql
-- tests/assert_less_than_5_pct_event_cookie_ids_are_null.sql

-- If >= 5% of cookie_ids are null, then the test returns 1 row (failure).
-- If < 5% of cookie_ids are null, then the test returns 0 rows (success)

with calc as (

    select
      sum(case when cookie_id is null then 1 else 0 end)::float / count(*)::float as fraction
    from {{ ref('events') }}

)

select * from calc where fraction < 0.05
```

To enable data tests, add the `test-paths` config to your `dbt_project.yml` file:

```yml
name: 'Vandelay Industries`
version: '1.0'

source-paths: ["models"]
target-path: "target"
test-paths: ["tests"]        # look for *.sql files in the "tests" directory
....
```

Any `.sql` file found in the `test-paths` director(y|ies) will be evaluated as data tests. These tests can be run with:

```bash
dbt test # run schema + data tests
dbt test --schema # run only schema tests
dbt test --data # run only data tests
dbt test --data --schema # run schema + data tests

# For more information, try
dbt test -h
```

### DBT-core integration tests

With the dbt 0.5.4 release, dbt now features a robust integration test suite. These integration tests will help mitigate the risk of software regressions, and in so doing, will help us develop dbt more quickly. You can check out the tests [here](https://github.com/fishtown-analytics/dbt/tree/development/test/integration), and the test results [here (linux/osx)](https://circleci.com/gh/fishtown-analytics/dbt/tree/master) and [here (windows)](https://ci.appveyor.com/project/DrewBanin/dbt/branch/development).


### The Future

You can check out the DBT roadmap [here](https://github.com/fishtown-analytics/dbt/milestones). In the next few weeks, we'll be working on [bugfixes](https://github.com/fishtown-analytics/dbt/milestone/11), [minor features](https://github.com/fishtown-analytics/dbt/milestone/15), [improved macro support](https://github.com/fishtown-analytics/dbt/milestone/14), and  [expanded control over runtime materialization configs](https://github.com/fishtown-analytics/dbt/milestone/9).

As always, feel free to reach out to us on [Slack](http://slack.getdbt.com/) with any questions or comments!

---

## dbt release 0.5.3

Bugfix release.

Fixes regressions introduced in 0.5.1 and 0.5.2.

### Fixed 0.5.1 regressions
Incremental models were broken by the new column expansion feature. Column expansion is implemented as
```sql
alter table ... add column tmp_col varchar({new_size});
update ... set tmp_col = existing_col
alter table ... drop column existing_col
alter table ... rename tmp_col to existing_col
```

This has the side-effect of moving the `existing_col` to the "end" of the table. When an incremental model tries to
```sql
insert into {table} (
   select * from tmp_table
)
```
suddenly the columns in `{table}` are incongruent with the columns in `tmp_table`. This insert subsequently fails.

The fix for this issue is twofold:

1. If the incremental model table DOES NOT already exist, avoid inserts altogether. Instead, run a `create table as (...)` statement
2. If the incremental model table DOES already exist, query for the columns in the existing table and use those to build the insert statement, eg:

```sql
insert into "dbt_dbanin"."sessions" ("session_end_tstamp", "session_start_tstamp", ...)
(
    select "session_end_tstamp", "session_start_tstamp", ...
    from "sessions__dbt_incremental_tmp"
);
```

In this way, the source and destination columns are guaranteed to be in the same order!

### Fixed 0.5.2 regressions

We attempted to refactor the way profiles work in dbt. Previously, a default `user` profile was loaded, and the profiles specified in `dbt_project.yml` or on the command line (`with --profile`) would be applied on top of the `user` config. This implementation is [some of the earliest code](https://github.com/fishtown-analytics/dbt/commit/430d12ad781a48af6a754442693834efdf98ffb1) that was committed to dbt.

As `dbt` has grown, we found this implementation to be a little unwieldy and hard to maintain. The 0.5.2 release made it so that only one profile could be loaded at a time. This profile needed to be specified in either `dbt_project.yml` or on the command line with `--profile`. A bug was errantly introduced during this change which broke the handling of dependency projects.

### The future

The additions of automated testing and a more comprehensive manual testing process will go a long way to ensuring the future stability of dbt. We're going to get started on these tasks soon, and you can follow our progress here: https://github.com/fishtown-analytics/dbt/milestone/16 .

As always, feel free to [reach out to us on Slack](http://slack.getdbt.com/) with any questions or concerns:




---

## dbt release 0.5.2

Patch release fixing a bug that arises when profiles are overridden on the command line with the `--profile` flag.

See https://github.com/fishtown-analytics/dbt/releases/tag/v0.5.1

---

## dbt release 0.5.1

### 0. tl;dr

1. Raiders of the Lost Archive -- version your raw data to make historical queries more accurate
2. Column type resolution for incremental models (no more `Value too long for character type` errors)
3. Postgres support
4. Top-level configs applied to your project + all dependencies
5. --threads CLI option + better multithreaded output

### 1. Source table archival https://github.com/fishtown-analytics/dbt/pull/183

Commonly, analysts need to "look back in time" at some previous state of data in their mutable tables. Imagine a `users` table which is synced to your data warehouse from a production database. This `users` table is a representation of what your users look like _now_. Consider what happens if you need to look at revenue by city for each of your users trended over time. Specifically, what happens if a user moved from, say, Philadelphia to New York? To do this correctly, you need to archive snapshots of the `users` table on a recurring basis. With this release, dbt now provides an easy mechanism to store such snapshots.

To use this new feature, declare the tables you want to archive in your `dbt_project.yml` file:

```yaml
archive:
    - source_schema: synced_production_data  # schema to look for tables in (declared below)
      target_schema: dbt_archive             # where to archive the data to
      tables:                                # list of tables to archive
        - source_table: users                # table to archive
          target_table: users_archived       # table to insert archived data into
          updated_at: updated_at             # used to determine when data has changed
          unique_key: id                     # used to generate archival query

        - source_table: some_other_table
           target_table: some_other_table_archive
           updated_at: "updatedAt"
           unique_key: "expressions || work || LOWER(too)"

    - source_schema: some_other_schema
      ....
```

The archived tables will mirror the schema of the source tables they're generated from. In addition, three fields are added to the archive table:

1. `valid_from`: The timestamp when this archived row was inserted (and first considered valid)
1. `valid_to`: The timestamp when this archived row became invalidated. The first archived record for a given `unique_key` has `valid_to = NULL`. When newer data is archived for that `unique_key`, the `valid_to` field of the old record is set to the `valid_from` field of the new record!
1. `scd_id`: A unique key generated for each archive record. Scd = [Slowly Changing Dimension](https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2:_add_new_row).

dbt models can be built on top of these archived tables. The most recent record for a given `unique_key` is the one where `valid_to` is `null`.

To run this archive process, use the command `dbt archive`. After testing and confirming that the archival works, you should schedule this process through cron (or similar).

### 2. Incremental column expansion https://github.com/fishtown-analytics/dbt/issues/175

Incremental tables are a powerful dbt feature, but there was at least one edge case which makes working with them difficult. During the first run of an incremental model, Redshift will infer a type for every column in the table. Subsequent runs can insert new data which does not conform to the expected type. One example is a `varchar(16)` field which is inserted into a `varchar(8)` field.
In practice, this error looks like:

```
Value too long for character type
DETAIL:
  -----------------------------------------------
  error:  Value too long for character type
  code:      8001
  context:   Value too long for type character varying(8)
  query:     3743263
  location:  funcs_string.hpp:392
  process:   query4_35 [pid=18194]
  -----------------------------------------------
```

With this release, dbt will detect when column types are incongruent and will attempt to reconcile these different types if possible. Specifically, dbt will alter the incremental model table schema from `character varying(x)` to `character varying(y)` for some `y > x`. This should drastically reduce the occurrence of this class of error.

### 3. First-class Postgres support https://github.com/fishtown-analytics/dbt/pull/183

With this release, Postgres became a first-class dbt target. You can configure a postgres database target in your `~/.dbt/profiles.yml` file:

```yaml
warehouse:
  outputs:
    dev:
      type: postgres    # configure a target for Postgres
      host: localhost
      user: Drew
      ....
  run-target: dev
```

While Redshift is built on top of Postgres, the two are subtly different. For instance, Redshift supports sort and dist keys, while Postgres does not! dbt will use the database target `type` parameter to generate the appropriate SQL for the target database.

### 4. Root-level configs https://github.com/fishtown-analytics/dbt/issues/161

Configurations in `dbt_project.yml` can now be declared at the `models:` level. These configurations will apply to the primary project, as well as any dependency projects. This feature is particularly useful for setting pre- or post- hooks that run for *every* model. In practice, this looks like:

```yaml
name: 'My DBT Project'

models:
    post-hook:
        - "grant select on {{this}} to looker_user"     # Applied to 'My DBT Project' and 'Snowplow' dependency
    'My DBT Project':
        enabled: true
    'Snowplow':
        enabled: true
```

### 5. --threads CLI option https://github.com/fishtown-analytics/dbt/issues/143

The number of threads that DBT uses can now be overridden with a CLI argument. The number of threads used must be between 1 and 8.

```bash
dbt run --threads 1    # fine
# or
dbt run --threads 4    # great
# or
dbt run --threads 42    # too many!
```

In addition to this new CLI argument, the output from multi-threaded dbt runs should be a little more orderly now. Models won't show as `START`ed until they're actually queued to run. Previously, the output here was a little confusing. Happy threading!

### Upgrading

To upgrade to version 0.5.1 of dbt, run:

``` bash
pip install --upgrade dbt
```

### And another thing

- Join us on [slack](http://slack.getdbt.com/) with questions or comments

Made with ♥️ by 🐟🏙  📈

---

### 0. tl;dr

- use a temp table when executing incremental models
- arbitrary configuration (using config variables)
- specify branches for dependencies
- more & better docs

### 1. new incremental model generation https://github.com/fishtown-analytics/dbt/issues/138

In previous versions of dbt, an edge case existed which caused the `sql_where` query to select different rows in the `delete` and `insert` steps. As a result, it was possible to construct incremental models which would insert duplicate records into the specified table. With this release, DBT uses a temp table which will 1) circumvent this issue and 2) improve query performance. For more information, check out the GitHub issue: https://github.com/fishtown-analytics/dbt/issues/138

### 2. Arbitrary configuration https://github.com/fishtown-analytics/dbt/issues/146

Configuration in dbt is incredibly powerful: it is what allows models to change their behavior without changing their code. Previously, all configuration was done using built-in parameters, but that actually limits the user in the power of configuration.

With this release, you can inject variables from `dbt_project.yml` into your top-level and dependency models. In practice, variables work like this:

```yml
# dbt_project.yml

models:
  my_project:
    vars:
      exclude_ip: '192.168.1.1'
```

```sql
-- filtered_events.sql

-- source code
select * from public.events where ip_address != '{{ var("exclude_ip") }}'

-- compiles to
select * from public.events where ip_address != '192.168.1.1'
```

The `vars` parameter in `dbt_project.yml` is compiled, so you can use jinja templating there as well! The primary use case for this is specifying "input" models to a dependency.

Previously, dependencies used `ref(...)` to select from a project's base models. That interface was brittle, and the idea that dependency code had unbridled access to all of your top-level models made us a little uneasy. As of this release, we're deprecating the ability for dependencies to `ref(...)` top-level models. Instead, the recommended way for this to work is with vars! An example:

```sql
-- dbt_modules/snowplow/models/events.sql

select * from {{ var('snowplow_events_table') }}
```

and

```yml
models:
  Snowplow:
    vars:
      snowplow_events_table: "{{ ref('base_events') }}"
```

This effectively mirrors the previous behavior, but it much more explicit about what's happening under the hood!

### 3. specify a dependency branch https://github.com/fishtown-analytics/dbt/pull/165

With this release, you can point DBT to a specific branch of a dependency repo. The syntax looks like this:

```
repositories:
    - https://github.com/fishtown-analytics/dbt-audit.git@development # use the "development" branch
```

### 4. More & Better Docs!

Check em out! And let us know if there's anything you think we can improve upon!


### Upgrading

To upgrade to version 0.5.0 of dbt, run:

``` bash
pip install --upgrade dbt
```

---

### 0. tl;dr

- `--version` command
- pre- and post- run hooks
- windows support
- event tracking


### 1. --version https://github.com/fishtown-analytics/dbt/issues/135

The `--version` command was added to help aid debugging. Further, organizations can use it to ensure that everyone in their org is up-to-date with dbt.

```bash
$ dbt --version
installed version: 0.4.7
   latest version: 0.4.7
Up to date!
```

### 2. pre-and-post-hooks https://github.com/fishtown-analytics/dbt/pull/147

With this release, you can now specify `pre-` and `post-` hooks that are run before and after a model is run, respectively. Hooks are useful for running `grant` statements, inserting a log of runs into an audit table, and more! Here's an example of a grant statement implemented using a post-hook:

```yml
models:
  my_project:
    post-hook: "grant select on table {{this}} to looker_user"
    my_model:
       materialized: view
    some_model:
      materialized: table
      post-hook: "insert into my_audit_table (model_name, run_at) values ({{this.name}}, getdate())"
```

Hooks are recursively appended, so the `my_model` model will only receive the `grant select...` hook, whereas the `some_model` model will receive _both_ the `grant select...` and `insert into...` hooks.

Finally, note that the `grant` statement uses the (hopefully familiar) `{{this}}` syntax whereas the `insert` statement uses the `{{this.name}}` syntax. When DBT creates a model:
 - A temp table is created
 - The original model is dropped
 - The temp table is renamed to the final model name

DBT will intelligently uses the right table/view name when you invoke `{{this}}`, but you have a couple of more specific options available if you need them:

```
{{this}} : "schema"."table__dbt_tmp"
{{this.schema}}: "schema"
{{this.table}}: "table__dbt_tmp"
{{this.name}}: "table"
```

### 3. Event tracking https://github.com/fishtown-analytics/dbt/issues/89

We want to build the best version of DBT possible, and a crucial part of that is understanding how users work with DBT. To this end, we've added some really simple event tracking to DBT (using Snowplow). We do not track credentials, model contents or model names (we consider these private, and frankly none of our business). This release includes basic event tracking that reports 1) when dbt is invoked 2) when models are run, and 3) basic platform information (OS + python version). The schemas for these events can be seen [here](https://github.com/fishtown-analytics/dbt/tree/development/events/schemas/com.fishtownanalytics)

You can opt out of event tracking at any time by adding the following to the top of you `~/.dbt/profiles.yml` file:

```yaml
config:
    send_anonymous_usage_stats: False
```

### 4. Windows support https://github.com/fishtown-analytics/dbt/pull/154

![windows](https://pbs.twimg.com/profile_images/571398080688181248/57UKydQS.png)

---

dbt v0.4.1 provides improvements to incremental models, performance improvements, and ssh support for db connections.

### 0. tl;dr

- slightly modified dbt command structure
- `unique_key` setting for incremental models
- connect to your db over ssh
- no more model-defaults
- multithreaded schema tests

If you encounter an SSL/cryptography error while upgrading to this version of dbt, check that your version of pip is up-to-date

```bash
pip install -U pip
pip install -U dbt
```

### 1. new dbt command structure https://github.com/fishtown-analytics/dbt/issues/109
```bash
# To run models
dbt run # same as before

# to dry-run models
dbt run --dry # previously dbt test

# to run schema tests
dbt test # previously dbt test --validate
```

### 2. Incremental model improvements https://github.com/fishtown-analytics/dbt/issues/101

Previously, dbt calculated "new" incremental records to insert by querying for rows which matched some `sql_where` condition defined in the model configuration. This works really well for atomic datasets like a clickstream event log -- once inserted, these records will never change. Other datasets, like a sessions table comprised of many pageviews for many users, can change over time. Consider the following scenario:

User 1 Session 1 Event 1 @ 12:00
User 1 Session 1 Event 2 @ 12:01
-- dbt run --
User 1 Session 1 Event 3 @ 12:02

In this scenario, there are two possible outcomes depending on the `sql_where` chosen: 1) Event 3 does not get included in the Session 1 record for User 1 (bad), or 2) Session 1 is duplicated in the sessions table (bad). Both of these outcomes are inadequate!

With this release, you can now add a `unique_key` expression to an incremental model config. Records matching the `unique_key` will be `delete`d from the incremental table, then `insert`ed as usual. This makes it possible to maintain data accuracy without recalculating the entire table on every run.

The `unique_key` can be any expression which uniquely defines the row, eg:
```yml
sessions:
  materialized: incremental
  sql_where: "session_end_tstamp > (select max(session_end_tstamp) from {{this}})"
  unique_key: user_id || session_index
```

### 3. Run schema validations concurrently https://github.com/fishtown-analytics/dbt/issues/100

The `threads` run-target config now applies to schema validations too. Try it with `dbt test`

### 4. Connect to database over ssh https://github.com/fishtown-analytics/dbt/issues/93

Add an `ssh-host` parameter to a run-target to connect to a database over ssh. The `ssh-host` parameter should be the name of a `Host` in your `~/.ssh/config` file [more info](http://nerderati.com/2011/03/17/simplify-your-life-with-an-ssh-config-file/)

```yml
warehouse:
  outputs:
    dev:
      type: redshift
      host: my-redshift.amazonaws.com
      port: 5439
      user: my-user
      pass: my-pass
      dbname: my-db
      schema: dbt_dbanin
      threads: 8
      ssh-host: ssh-host-name  # <------ Add this line
  run-target: dev
```

### Remove the model-defaults config https://github.com/fishtown-analytics/dbt/issues/111

The `model-defaults` config doesn't make sense in a dbt world with dependencies. To apply default configs to your package, add the configs immediately under the package definition:

```yml
models:
    My_Package:
        enabled: true
        materialized: table
        snowplow:
            ...
```

---

## dbt v0.4.0

dbt v0.4.0 provides new ways to materialize models in your database.

### 0. tl;dr
 - new types of materializations: `incremental` and `ephemeral`
 - if upgrading, change `materialized: true|false` to `materialized: table|view|incremental|ephemeral`
 - optionally specify model configs within the SQL file

### 1. Feature: `{{this}}` template variable https://github.com/fishtown-analytics/dbt/issues/81
The `{{this}}` template variable expands to the name of the model being compiled. For example:

```sql
-- my_model.sql
select 'the fully qualified name of this model is {{ this }}'
-- compiles to
select 'the fully qualified name of this model is "the_schema"."my_model"'
```

### 2. Feature: `materialized: incremental` https://github.com/fishtown-analytics/dbt/pull/90

After initially creating a table, incremental models will `insert` new records into the table on subsequent runs. This drastically speeds up execution time for large, append-only datasets.

Each execution of dbt run will:
 - create the model table if it doesn't exist
 - insert new records into the table

New records are identified by a `sql_where` model configuration option. In practice, this looks like:

```yml

sessions:
    materialized: incremental
    sql_where: "session_start_time > (select max(session_start_time) from {{this}})"
```

There are a couple of new things here. Previously, `materialized` could either be set to `true` or `false`. Now, the valid options include `view`, `table,` `incremental`, and `ephemeral` (more on this last one below). Also note that incremental models generally require use of the {{this}} template variable to identify new records.

The `sql_where` field is supplied as a `where` condition on a subquery containing the model definition. This resultset is then inserted into the target model. This looks something like:

```sql
insert into schema.model (
    select * from (
        -- compiled model definition
    ) where {{sql_where}}
)
```

### 3. Feature: `materialized: ephemeral` https://github.com/fishtown-analytics/dbt/issues/78

Ephemeral models are injected as CTEs (`with` statements) into any model that `ref`erences them. Ephemeral models are part of the dependency graph and generally function like any other model, except ephemeral models are not compiled to their own files or directly created in the database. This is useful for intermediary models which are shared by other downstream models, but shouldn't be queried directly from outside of dbt.

To make a model ephemeral:

```yml
employees:
    materialized: ephemeral
```

Suppose you wanted to exclude `employees` from your `users` table, but you don't want to clutter your analytics schema with an `employees` table.

```sql
-- employees.sql
select * from public.employees where is_deleted = false

-- users.sql
select *
from {{ref('users')}}
where email not in (select email from {{ref('employees')}})
```

The compiled SQL would look something like:
```sql
with __dbt__CTE__employees as (
  select * from public.employees where is_deleted = false
)
select *
from users
where email not in (select email from __dbt__CTE__employees)
```

Ephemeral models play nice with other ephemeral models, incremental models, and regular table/view models. Feel free to mix and match different materialization options to optimize for performance and simplicity.


### 4. Feature: In-model configs https://github.com/fishtown-analytics/dbt/issues/88

Configurations can now be specified directly inside of models. These in-model configs work exactly the same as configs inside of the dbt_project.yml file.

An in-model-config looks like this:

```sql
-- users.sql

-- python function syntax
{{ config(materialized="incremental", sql_where="id > (select max(id) from {{this}})") }}
-- OR json syntax
{{
    config({"materialized:" "incremental", "sql_where" : "id > (select max(id) from {{this}})"})
}}

select * from public.users
```

The config resolution order is:
  1. dbt_project.yml `model-defaults`
  2. in-model config
  3. dbt_project.yml `models` config

### 5. Fix: dbt seed null values https://github.com/fishtown-analytics/dbt/issues/102

Previously, `dbt seed` would insert empty CSV cells as `"None"`, whereas they should have been `NULL`. Not anymore!


---

## dbt v0.3.0

Version 0.3.0 comes with the following updates:

#### 1. Parallel model creation https://github.com/fishtown-analytics/dbt/pull/83
dbt will analyze the model dependency graph and can create models in parallel if possible. In practice, this can significantly speed up the amount of time it takes to complete `dbt run`. The number of threads dbt uses must be between 1 and 8. To configure the number of threads dbt uses, add the `threads` key to your dbt target in `~/.dbt/profiles.yml`, eg:

```yml
user:
  outputs:
    my-redshift:
      type: redshift
      threads: 4         # execute up to 4 models concurrently
      host: localhost
      ...
  run-target: my-redshift
```

For a complete example, check out [a sample profiles.yml file](https://github.com/fishtown-analytics/dbt/blob/master/sample.profiles.yml)

#### 2. Fail only within a single dependency chain https://github.com/fishtown-analytics/dbt/issues/63
If a model cannot be created, it won't crash the entire `dbt run` process. The errant model will fail and all of its descendants will be "skipped". Other models which do not depend on the failing model (or its descendants) will still be created.

#### 3. Logging https://github.com/fishtown-analytics/dbt/issues/64, https://github.com/fishtown-analytics/dbt/issues/65
dbt will log output from the `dbt run` and `dbt test` commands to a configurable logging directory. By default, this directory is called `logs/`. The log filename is `dbt.log` and it is rotated on a daily basic. Logs are kept for 7 days.

To change the name of the logging directory, add the following line to your `dbt_project.yml` file:
```yml
log-path: "my-logging-directory" # will write logs to my-logging-directory/dbt.log
```

#### 4. Minimize time models are unavailable in the database https://github.com/fishtown-analytics/dbt/issues/68
Previously, dbt would create models by:
1. dropping the existing model
2. creating the new model

This resulted in a significant amount of time in which the model was inaccessible to the outside world. Now, dbt creates models by:
1. creating a temporary model `{model-name}__dbt_tmp`
2. dropping the existing model
3. renaming the tmp model name to the actual model name

#### 5. Arbitrarily deep nesting https://github.com/fishtown-analytics/dbt/issues/50
Previously, all models had to be located in a directory matching `models/{model group}/{model_name}.sql`. Now, these models can be nested arbitrarily deeply within a given dbt project. For instance, `models/snowplow/sessions/transformed/transformed_sessions.sql` is a totally valid model location with this release.

To configure these deeply-nested models, just nest the config options within the `dbt_project.yml` file. The only caveat is that you need to specify the dbt project name as the first key under the `models` object, ie:

```yml
models:
  'Your Project Name':
    snowplow:
      sessions:
        transformed:
          transformed_sessions:
            enabled: true
```

More information is available on the [issue](https://github.com/fishtown-analytics/dbt/issues/50) and in the [sample dbt_project.yml file](https://github.com/fishtown-analytics/dbt/blob/master/sample.dbt_project.yml)

#### 6. don't try to create a schema if it already exists https://github.com/fishtown-analytics/dbt/issues/66
dbt run would execute `create schema if not exists {schema}`. This would fail if the dbt user didn't have sufficient permissions to create the schema, even if the schema already existed! Now, dbt checks for the schema existence and only attempts to create the schema if it doesn't already exist.

#### 7. Semantic Versioning
The previous release of dbt was v0.2.3.0 which isn't a semantic version. This and all future dbt releases will conform to semantic version in the format `{major}.{minor}.{patch}`.
---

## dbt v0.2.3.0
Version 0.2.3.0 of dbt comes with the following updates:

#### 1. Fix: Flip referential integrity arguments (breaking)
Referential integrity validations in a `schema.yml` file were previously defined relative to the *parent* table:
```yaml
account:
  constraints:
    relationships:
      - {from: id, to: people, field: account_id}
```

Now, these validations are specified relative to the *child* table
```yaml
people:
  constraints:
    relationships:
      - {from: account_id, to: accounts, field: id}
```

For more information, run `dbt test -h`

#### 2. Feature: seed tables from a CSV
Previously, auxiliary data needed to be shoehorned into a view comprised of union statements, eg.
```sql
select 22 as "type", 'Chat Transcript' as type_name, 'chatted via olark' as event_name union all
select 21, 'Custom Redirect', 'clicked a custom redirect' union all
select 6, 'Email', 'email sent' union all
...
```

That's not a scalable solution. Now you can load CSV files into your data warehouse:
1. Add a CSV file (with a header) to the `data/` directory
2. Run `dbt seed` to create a table from the CSV file!
3. The table name with be the filename (sans `.csv`) and it will be placed in your `run-target`'s schema

Subsequent calls to `dbt seed` will truncate the seeded tables (if they exist) and re-insert the data. If the table schema changes, you can run `dbt seed --drop-existing` to drop the table and recreate it.

For more information, run `dbt seed -h`

#### 3. Feature: compile analytical queries

Versioning your SQL models with dbt is a great practice, but did you know that you can also version your analyses? Any SQL files in the `analysis/` dir will be compiled (ie. table names will be interpolated) and placed in the `target/build-analysis/` directory. These analytical queries will _not_ be run against your data warehouse with `dbt run` -- you should copy/paste them into the data analysis tool of your choice.

#### 4. Feature: accepted values validation

In your `schema.yml` file, you can now add `accepted-values` validations:
```yaml
accounts:
  constraints:
    accepted-values:
      - {field: type, values: ['paid', 'free']}
```

This test will determine how many records in the `accounts` model have a `type` other than `paid` or `free`.

#### 5. Feature: switch profiles and targets on the command line

Switch between profiles with `--profile [profile-name]` and switch between run-targets with `--target [target-name]`.

Targets should be something like "prod" or "dev" and profiles should be something like "my-org" or "my-side-project"

```yaml
side-project:
  outputs:
    prod:
      type: redshift
      host: localhost
      port: 5439
      user: Drew
      pass:
      dbname: data_generator
      schema: ac_drew
    dev:
      type: redshift
      host: localhost
      port: 5439
      user: Drew
      pass:
      dbname: data_generator
      schema: ac_drew_dev
  run-target: dev
```

To compile models using the `dev` environment of my `side-project` profile:
`$ dbt compile --profile side-project --target dev`
or for `prod`:
`$ dbt compile --profile side-project --target prod`

You can also add a "profile' config to the `dbt_config.yml` file to fix a dbt project to a specific profile:

```yaml
...
test-paths: ["test"]
data-paths: ["data"]

# Fix this project to the "side-project" profile
# You can still use --target to switch between environments!
profile: "side-project"

model-defaults:
....
```
