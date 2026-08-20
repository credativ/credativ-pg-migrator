import unittest
from unittest.mock import MagicMock, patch
from credativ_pg_migrator.connectors.postgresql_connector import PostgreSQLConnector


def build_connector(names_case='keep'):
    mock_config = MagicMock()
    mock_config.get_log_file.return_value = 'migrator.log'
    mock_config.get_source_db_type.return_value = 'postgresql'
    mock_config.get_names_case_handling.return_value = names_case
    mock_config.convert_names_case.side_effect = lambda x: x
    with patch('credativ_pg_migrator.connectors.postgresql_connector.MigratorLogger'), \
         patch.object(PostgreSQLConnector, 'prepare_session_settings', return_value=''):
        connector = PostgreSQLConnector(mock_config, 'source')
    return connector


def aggregate_row(**overrides):
    row = {
        'aggregate_schema': 'public', 'aggregate_name': 'weighted_avg',
        'arguments': 'numeric, numeric', 'aggkind': 'n',
        'sfunc': 'public.weighted_sum_accum', 'stype': 'numeric[]', 'sspace': None,
        'finalfunc': 'public.weighted_sum_final', 'finalextra': False, 'finalmodify': 'r',
        'combinefunc': None, 'serialfunc': None, 'deserialfunc': None, 'initcond': '{0,0}',
        'msfunc': None, 'minvfunc': None, 'mstype': None, 'msspace': None,
        'mfinalfunc': None, 'mfinalextra': False, 'mfinalmodify': None, 'minitcond': None,
        'sortop': None, 'proparallel': 's',
    }
    row.update(overrides)
    return tuple(row[key] for key in (
        'aggregate_schema', 'aggregate_name', 'arguments', 'aggkind', 'sfunc', 'stype',
        'sspace', 'finalfunc', 'finalextra', 'finalmodify', 'combinefunc', 'serialfunc',
        'deserialfunc', 'initcond', 'msfunc', 'minvfunc', 'mstype', 'msspace',
        'mfinalfunc', 'mfinalextra', 'mfinalmodify', 'minitcond', 'sortop', 'proparallel'))


class TestCreateAggregateSql(unittest.TestCase):

    def test_aggregate_with_state_and_final_function(self):
        connector = build_connector()
        sql = connector.get_create_aggregate_sql(aggregate_row())
        self.assertIn('CREATE AGGREGATE public.weighted_avg(numeric, numeric) (', sql)
        self.assertIn('SFUNC = public.weighted_sum_accum', sql)
        self.assertIn('STYPE = numeric[]', sql)
        self.assertIn('FINALFUNC = public.weighted_sum_final', sql)
        self.assertIn("INITCOND = '{0,0}'", sql)
        self.assertIn('PARALLEL = SAFE', sql)
        self.assertTrue(sql.rstrip().endswith(');'))

    def test_minimal_aggregate(self):
        connector = build_connector()
        sql = connector.get_create_aggregate_sql(aggregate_row(
            aggregate_name='sum_money', arguments='public.money_amount',
            sfunc='public.money_add', stype='public.money_amount',
            finalfunc=None, finalmodify=None, initcond=None, proparallel='u'))
        self.assertEqual(
            sql,
            'CREATE AGGREGATE public.sum_money(public.money_amount) (\n'
            '    SFUNC = public.money_add,\n'
            '    STYPE = public.money_amount\n);')

    def test_unsafe_parallel_is_not_emitted(self):
        # UNSAFE is the default, emitting it only adds noise
        connector = build_connector()
        self.assertNotIn('PARALLEL', connector.get_create_aggregate_sql(aggregate_row(proparallel='u')))

    def test_restricted_parallel_is_emitted(self):
        connector = build_connector()
        self.assertIn('PARALLEL = RESTRICTED',
                      connector.get_create_aggregate_sql(aggregate_row(proparallel='r')))

    def test_finalfunc_extra_and_modify(self):
        connector = build_connector()
        sql = connector.get_create_aggregate_sql(aggregate_row(finalextra=True, finalmodify='w'))
        self.assertIn('FINALFUNC_EXTRA', sql)
        self.assertIn('FINALFUNC_MODIFY = READ_WRITE', sql)

    def test_parallel_aggregate_support_functions(self):
        connector = build_connector()
        sql = connector.get_create_aggregate_sql(aggregate_row(
            combinefunc='public.my_combine', serialfunc='public.my_serial',
            deserialfunc='public.my_deserial', sspace=128))
        self.assertIn('SSPACE = 128', sql)
        self.assertIn('COMBINEFUNC = public.my_combine', sql)
        self.assertIn('SERIALFUNC = public.my_serial', sql)
        self.assertIn('DESERIALFUNC = public.my_deserial', sql)

    def test_moving_aggregate_implementation(self):
        connector = build_connector()
        sql = connector.get_create_aggregate_sql(aggregate_row(
            msfunc='public.m_accum', minvfunc='public.m_inv', mstype='numeric[]',
            msspace=64, mfinalfunc='public.m_final', mfinalextra=True,
            mfinalmodify='s', minitcond='{0,0}'))
        for expected in ('MSFUNC = public.m_accum', 'MINVFUNC = public.m_inv',
                         'MSTYPE = numeric[]', 'MSSPACE = 64',
                         'MFINALFUNC = public.m_final', 'MFINALFUNC_EXTRA',
                         'MFINALFUNC_MODIFY = SHAREABLE', "MINITCOND = '{0,0}'"):
            self.assertIn(expected, sql)

    def test_sort_operator(self):
        connector = build_connector()
        self.assertIn('SORTOP = OPERATOR(<)',
                      connector.get_create_aggregate_sql(aggregate_row(sortop='<')))

    def test_ordered_set_aggregate(self):
        connector = build_connector()
        sql = connector.get_create_aggregate_sql(aggregate_row(
            aggregate_name='my_percentile', aggkind='o',
            arguments='double precision ORDER BY double precision'))
        self.assertIn('my_percentile(double precision ORDER BY double precision)', sql)
        self.assertNotIn('HYPOTHETICAL', sql)

    def test_hypothetical_set_aggregate(self):
        connector = build_connector()
        sql = connector.get_create_aggregate_sql(aggregate_row(aggkind='h'))
        self.assertIn('HYPOTHETICAL', sql)

    def test_initcond_with_quote_is_escaped(self):
        connector = build_connector()
        self.assertIn("INITCOND = '{''a''}'",
                      connector.get_create_aggregate_sql(aggregate_row(initcond="{'a'}")))


class TestFetchFuncprocNames(unittest.TestCase):

    def _fetch(self, rows):
        connector = build_connector()
        connector.connect = MagicMock()
        connector.disconnect = MagicMock()
        cursor = MagicMock()
        cursor.fetchall.return_value = rows
        connector.connection = MagicMock()
        connector.connection.cursor.return_value = cursor
        return connector.fetch_funcproc_names('public'), cursor

    def test_aggregates_are_recognized_and_carry_their_arguments(self):
        rows = [
            (1, 'my_func', 'integer', None, 'f'),
            (2, 'my_proc', 'text', None, 'p'),
            (3, 'weighted_avg', 'numeric, numeric', 'a comment', 'a'),
        ]
        funcprocs, cursor = self._fetch(rows)
        self.assertEqual([item['type'] for item in funcprocs.values()],
                         ['FUNCTION', 'PROCEDURE', 'AGGREGATE'])
        self.assertEqual(funcprocs[3]['arguments'], 'numeric, numeric')
        self.assertEqual(funcprocs[3]['header'], 'weighted_avg(numeric, numeric)')

    def test_query_orders_aggregates_last_and_skips_extension_routines(self):
        _, cursor = self._fetch([])
        query = cursor.execute.call_args[0][0]
        self.assertIn("p.prokind IN ('f', 'p', 'a')", query)
        self.assertIn("ORDER BY (p.prokind = 'a'), p.proname", query)
        self.assertIn("deptype = 'e'", query)


if __name__ == '__main__':
    unittest.main()
