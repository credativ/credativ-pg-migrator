# SPDX-License-Identifier: GPL-3.0-or-later
"""
The session settings of the configuration - target -> settings, and the same key for a
PostgreSQL source.

Two things are tested: what is prepared out of the configuration (which settings, in which
order, read from which section), and that every connection runs with them. The second is
what decides who owns the objects the migration creates: the role of the settings is only
the owner of an object if the connection which created it was opened with that role.

Run with:  python3 -m pytest tests/test_pg_session_settings.py -v
"""

import unittest
from unittest.mock import MagicMock, patch

from credativ_pg_migrator.connectors.postgresql_connector import PostgreSQLConnector


class FakeCursor:
    """Answers the pg_settings lookup with the names PostgreSQL is taken to know."""

    KNOWN = {'work_mem', 'maintenance_work_mem', 'role', 'search_path', 'statement_timeout'}

    def __init__(self, executed):
        self.executed = executed
        self.rows = []

    def execute(self, query, params=None):
        self.executed.append(query)
        if 'pg_settings' in query:
            asked = params[0] if params else ()
            self.rows = [(name,) for name in asked if name in self.KNOWN]

    def fetchall(self):
        return self.rows

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exception):
        return False


class FakeConnection:
    def __init__(self, executed):
        self.executed = executed
        self.autocommit = False
        self.closed = False

    def cursor(self):
        return FakeCursor(self.executed)

    def close(self):
        self.closed = True


def build_connector(source_or_target='target', config=None, executed=None):
    """A connector whose connections are recorded instead of opened."""
    executed = executed if executed is not None else []
    settings = config if config is not None else {}
    mock_config = MagicMock()
    mock_config.get_log_file.return_value = 'migrator.log'
    mock_config.get_connect_string.return_value = 'dbname=x'
    mock_config.get_db_session_settings.side_effect = lambda direction: settings.get(direction, {})
    messages = []
    mock_config.print_log_message.side_effect = lambda level, message: messages.append((level, message))

    with patch('credativ_pg_migrator.connectors.postgresql_connector.MigratorLogger'), \
         patch('credativ_pg_migrator.connectors.postgresql_connector.psycopg2.connect',
               side_effect=lambda *a, **k: FakeConnection(executed)), \
         patch.object(PostgreSQLConnector, '_register_type_casters', lambda self: None):
        connector = PostgreSQLConnector(mock_config, source_or_target)
    connector._messages = messages
    connector._executed = executed
    return connector


def connect_again(connector):
    """Open one more connection, the way every worker of the migration does."""
    with patch('credativ_pg_migrator.connectors.postgresql_connector.psycopg2.connect',
               side_effect=lambda *a, **k: FakeConnection(connector._executed)), \
         patch.object(PostgreSQLConnector, '_register_type_casters', lambda self: None):
        connector.connect()


class TestPreparedSettings(unittest.TestCase):

    def test_the_settings_of_the_section_of_this_connection_are_used(self):
        """
        The role and the search_path of the target have no business on the connection to the
        source - they were applied on both, so a PostgreSQL source was read under the role
        meant for writing the target.
        """
        config = {'target': {'work_mem': '32MB'}, 'source': {'statement_timeout': '5min'}}
        target = build_connector('target', config)
        source = build_connector('source', config)
        self.assertIn("SET work_mem = '32MB';", target.session_settings)
        self.assertNotIn('statement_timeout', target.session_settings)
        self.assertIn("SET statement_timeout = '5min';", source.session_settings)
        self.assertNotIn('work_mem', source.session_settings)

    def test_a_name_is_recognised_whatever_its_case(self):
        """A key written 'Role' or 'WORK_MEM' used to be a KeyError."""
        connector = build_connector('target', {'target': {'WORK_MEM': '32MB', 'Role': 'app_owner'}})
        self.assertIn("SET WORK_MEM = '32MB';", connector.session_settings)
        self.assertIn("SET Role = 'app_owner';", connector.session_settings)

    def test_the_role_is_applied_last(self):
        """A setting which needs more rights than the role has must be set before the switch."""
        connector = build_connector('target', {'target': {'role': 'app_owner', 'work_mem': '32MB',
                                                          'maintenance_work_mem': '512MB'}})
        statements = [s for s in connector.session_settings.split(';') if s.strip()]
        self.assertTrue(statements[-1].strip().lower().startswith('set role'), statements)

    def test_search_path_is_written_without_quotes_of_its_own(self):
        connector = build_connector('target', {'target': {'search_path': 'migtest, public'}})
        self.assertIn('SET search_path = migtest, public;', connector.session_settings)

    def test_an_unknown_name_is_reported_and_not_applied(self):
        connector = build_connector('target', {'target': {'work_mem': '32MB', 'wrok_mem': '32MB'}})
        self.assertIn("SET work_mem = '32MB';", connector.session_settings)
        self.assertNotIn('wrok_mem', connector.session_settings)
        self.assertTrue(any(level == 'WARNING' and 'wrok_mem' in message
                            for level, message in connector._messages), connector._messages)

    def test_no_settings_at_all_prepares_nothing(self):
        connector = build_connector('target', {'target': {}})
        self.assertEqual(connector.session_settings, '')


class TestEverySessionRunsWithThem(unittest.TestCase):

    def test_connect_applies_them(self):
        """
        Every connection, not only the handful of places which were patched to do it: the
        objects created by all the others were owned by the login role instead of the role of
        the configuration.
        """
        connector = build_connector('target', {'target': {'role': 'app_owner'}})
        connector._executed.clear()
        connect_again(connector)
        self.assertIn("SET role = 'app_owner';", connector._executed)

    def test_connect_applies_nothing_when_there_is_nothing_to_apply(self):
        connector = build_connector('target', {'target': {}})
        connector._executed.clear()
        connect_again(connector)
        self.assertEqual(connector._executed, [])

    def test_preparing_the_settings_does_not_recurse(self):
        """
        prepare_session_settings() opens a connection of its own to ask pg_settings, and
        connect() applies what is prepared - the attribute has to exist, and be empty, before
        that first connection is opened.
        """
        connector = build_connector('target', {'target': {'role': 'app_owner'}})
        self.assertFalse(any(statement.startswith('SET role') for statement in connector._executed),
                         connector._executed)


if __name__ == '__main__':
    unittest.main()
