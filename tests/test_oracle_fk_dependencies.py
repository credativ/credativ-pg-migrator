# SPDX-License-Identifier: GPL-3.0-or-later
"""
The foreign key ranking of the pre-migration analysis, for Oracle - P3-4.

`get_top_fk_dependencies()` used to return `{}`, so an Oracle schema was surveyed without a
word about its foreign keys and the planner printed "No foreign key dependencies found in
source database" whatever the schema held.

What is asserted here is the shape the planner prints and the three things the ranking is
supposed to say beyond a count: the order (most keys first, then by name, so two runs over
the same schema read the same), that a referenced table which is not migrated is marked, and
that a table removed by the filters is not ranked at all.

Run with:  python3 -m pytest tests/test_oracle_fk_dependencies.py -v
"""

import os
import sys
import types
import unittest
from unittest.mock import MagicMock

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

try:
    import oracledb  # noqa: F401
except ModuleNotFoundError:
    ## The connector imports the Oracle driver at module level, and the driver is not a
    ## dependency of this migrator - it is installed by whoever migrates an Oracle database.
    ## Nothing here reaches it: the connector is built with __new__ and its cursor is a mock,
    ## so an empty module is enough to import the file. This is what keeps the test suite
    ## free of a driver, the way it is for every other connector.
    sys.modules['oracledb'] = types.ModuleType('oracledb')

from credativ_pg_migrator.connectors.oracle_connector import OracleConnector


## owner, table, constraint, fk columns, referenced owner, referenced table, referenced columns
CONSTRAINT_ROWS = [
    ('HR', 'EMPLOYEES', 'EMP_DEPT_FK', 'DEPARTMENT_ID', 'HR', 'DEPARTMENTS', 'DEPARTMENT_ID'),
    ('HR', 'EMPLOYEES', 'EMP_JOB_FK', 'JOB_ID', 'HR', 'JOBS', 'JOB_ID'),
    ('HR', 'EMPLOYEES', 'EMP_MGR_FK', 'MANAGER_ID', 'HR', 'EMPLOYEES', 'EMPLOYEE_ID'),
    ('HR', 'DEPARTMENTS', 'DEPT_LOC_FK', 'LOCATION_ID', 'HR', 'LOCATIONS', 'LOCATION_ID'),
    ('HR', 'JOB_HISTORY', 'JHIST_JOB_FK', 'JOB_ID', 'HR', 'JOBS', 'JOB_ID'),
    ## composite key, and a referenced table in another schema
    ('HR', 'JOB_HISTORY', 'JHIST_EMP_FK', 'EMPLOYEE_ID, START_DATE', 'OE', 'ASSIGNMENTS', 'EMPLOYEE_ID, START_DATE'),
    ## the referenced constraint is not readable through ALL_CONSTRAINTS
    ('HR', 'LOCATIONS', 'LOC_C_FK', 'COUNTRY_ID', None, None, None),
    ## excluded from the migration by the table filters
    ('HR', 'AUDIT_LOG', 'AUDIT_EMP_FK', 'EMPLOYEE_ID', 'HR', 'EMPLOYEES', 'EMPLOYEE_ID'),
]


def build_connector(rows=CONSTRAINT_ROWS, excluded=('AUDIT_LOG',), execute_raises=None):
    connector = OracleConnector.__new__(OracleConnector)
    connector.config_parser = MagicMock()
    connector.config_parser.is_object_selected.side_effect = (
        lambda kind, name: (False, 'it is matched by exclude_tables')
        if name in excluded else (True, None))
    connector.connect = MagicMock()
    connector.disconnect = MagicMock()

    cursor = MagicMock()
    if execute_raises is not None:
        cursor.execute.side_effect = execute_raises
    cursor.fetchall.return_value = rows
    connector.connection = MagicMock()
    connector.connection.cursor.return_value = cursor
    return connector, cursor


class TestOracleTopFkDependencies(unittest.TestCase):

    def test_ranking_counts_the_keys_defined_on_each_table(self):
        connector, cursor = build_connector()

        result = connector.get_top_fk_dependencies({'source_schema_name': 'hr'})

        ## the schema is bound upper case, the way every other query of the connector binds it
        self.assertEqual(cursor.execute.call_args[0][1], {'owner': 'HR'})

        self.assertEqual([entry['table_name'] for entry in result.values()],
                         ['EMPLOYEES', 'JOB_HISTORY', 'DEPARTMENTS', 'LOCATIONS'])
        self.assertEqual([entry['fk_count'] for entry in result.values()], [3, 2, 1, 1])
        self.assertEqual(list(result.keys()), [1, 2, 3, 4])
        self.assertEqual(result[1]['owner'], 'HR')

    def test_dependencies_name_both_ends_of_every_key(self):
        connector, _cursor = build_connector()

        result = connector.get_top_fk_dependencies({'source_schema_name': 'HR'})

        self.assertEqual(
            result[1]['dependencies'],
            'EMPLOYEES.DEPARTMENT_ID -> DEPARTMENTS.DEPARTMENT_ID, '
            'EMPLOYEES.JOB_ID -> JOBS.JOB_ID, '
            'EMPLOYEES.MANAGER_ID -> EMPLOYEES.EMPLOYEE_ID')

    def test_a_composite_key_is_written_as_a_list_and_another_schema_is_marked(self):
        connector, _cursor = build_connector()

        result = connector.get_top_fk_dependencies({'source_schema_name': 'HR'})
        job_history = result[2]['dependencies']

        self.assertIn('JOB_HISTORY.JOB_ID -> JOBS.JOB_ID', job_history)
        ## a table of another schema is not migrated, so the key cannot be created either
        self.assertIn('JOB_HISTORY(EMPLOYEE_ID, START_DATE) -> '
                      'OE.ASSIGNMENTS(EMPLOYEE_ID, START_DATE) [not migrated]', job_history)

    def test_a_referenced_constraint_which_cannot_be_read_still_counts(self):
        """
        The migrator reads foreign keys through the same join, so a key whose referenced
        constraint is invisible to this account is not migrated - dropping it from the count
        would make the ranking read as if the table had fewer keys than it has.
        """
        connector, _cursor = build_connector()

        result = connector.get_top_fk_dependencies({'source_schema_name': 'HR'})
        locations = [entry for entry in result.values() if entry['table_name'] == 'LOCATIONS'][0]

        self.assertEqual(locations['fk_count'], 1)
        self.assertIn('not visible to this account', locations['dependencies'])

    def test_a_table_left_out_by_the_filters_is_not_ranked(self):
        connector, _cursor = build_connector()

        result = connector.get_top_fk_dependencies({'source_schema_name': 'HR'})

        self.assertNotIn('AUDIT_LOG', [entry['table_name'] for entry in result.values()])

    def test_a_key_pointing_at_an_excluded_table_is_marked(self):
        connector, _cursor = build_connector(excluded=('JOBS',))

        result = connector.get_top_fk_dependencies({'source_schema_name': 'HR'})

        self.assertIn('EMPLOYEES.JOB_ID -> JOBS.JOB_ID [not migrated]',
                      result[1]['dependencies'])

    def test_only_the_top_n_tables_are_returned(self):
        connector, _cursor = build_connector()
        connector.TOP_FK_DEPENDENCIES_COUNT = 2

        result = connector.get_top_fk_dependencies({'source_schema_name': 'HR'})

        self.assertEqual(len(result), 2)
        self.assertEqual([entry['table_name'] for entry in result.values()],
                         ['EMPLOYEES', 'JOB_HISTORY'])

    def test_the_whole_schema_is_read_when_none_is_given(self):
        connector, cursor = build_connector()

        connector.get_top_fk_dependencies({})

        self.assertEqual(cursor.execute.call_args[0][1], {'owner': None})

    def test_a_failed_read_is_reported_and_does_not_raise(self):
        """
        The analysis is a read-only survey and must not stop the migration - but an empty
        ranking must not be the only thing the log says, or 'not read' is indistinguishable
        from 'no foreign keys'.
        """
        connector, _cursor = build_connector(execute_raises=Exception('ORA-00942: table or view does not exist'))

        result = connector.get_top_fk_dependencies({'source_schema_name': 'HR'})

        self.assertEqual(result, {})
        levels = [call.args[0] for call in connector.config_parser.print_log_message.call_args_list]
        self.assertIn('ERROR', levels)
        reported = ' '.join(str(call.args[1]) for call in connector.config_parser.print_log_message.call_args_list
                            if call.args[0] == 'ERROR')
        self.assertIn('ORA-00942', reported)

    def test_a_schema_without_foreign_keys_gives_an_empty_ranking(self):
        connector, _cursor = build_connector(rows=[])

        self.assertEqual(connector.get_top_fk_dependencies({'source_schema_name': 'HR'}), {})


if __name__ == '__main__':
    unittest.main()
