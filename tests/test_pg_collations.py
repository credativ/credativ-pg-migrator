import unittest
from unittest.mock import MagicMock, patch
from credativ_pg_migrator.connectors.postgresql_connector import PostgreSQLConnector


def build_connector(source_db_type='postgresql', existing_collations=None, names_case='lower'):
    mock_config = MagicMock()
    mock_config.get_log_file.return_value = 'migrator.log'
    mock_config.get_source_db_type.return_value = source_db_type
    mock_config.get_names_case_handling.return_value = names_case
    if names_case == 'keep':
        mock_config.convert_names_case.side_effect = lambda x: x
    else:
        mock_config.convert_names_case.side_effect = lambda x: x.lower() if x else x
    with patch('credativ_pg_migrator.connectors.postgresql_connector.MigratorLogger'), \
         patch.object(PostgreSQLConnector, 'prepare_session_settings', return_value=''):
        connector = PostgreSQLConnector(mock_config, 'target')
    connector.existing_collation_names = existing_collations if existing_collations is not None else {
        'C', 'POSIX', 'ucs_basic', 'en_US.utf8'}
    return connector


USER_COLLATIONS = {
    'natural_numeric': {'target_schema_name': 'migtest', 'target_collation_name': 'natural_numeric'},
}


class TestPgCollations(unittest.TestCase):

    def test_parse_identifier_parts_keeps_dots_inside_quotes(self):
        connector = build_connector()
        self.assertEqual(connector.parse_identifier_parts('"en_US.utf8"'), ['en_US.utf8'])
        self.assertEqual(connector.parse_identifier_parts('"public"."natural_numeric"'), ['public', 'natural_numeric'])
        self.assertEqual(connector.parse_identifier_parts('C'), ['C'])

    def test_split_leading_identifier(self):
        connector = build_connector()
        self.assertEqual(connector.split_leading_identifier('"natural_numeric" text_pattern_ops'),
                         ('"natural_numeric"', 'text_pattern_ops'))
        self.assertEqual(connector.split_leading_identifier('"en_US.utf8"'), ('"en_US.utf8"', ''))

    def test_migrated_collation_is_qualified_with_target_schema(self):
        connector = build_connector()
        self.assertEqual(connector.get_collate_clause('"natural_numeric"', USER_COLLATIONS),
                         ' COLLATE "migtest"."natural_numeric"')

    def test_builtin_collation_is_kept_untouched(self):
        connector = build_connector()
        self.assertEqual(connector.get_collate_clause('"en_US.utf8"', USER_COLLATIONS), ' COLLATE "en_US.utf8"')
        self.assertEqual(connector.get_collate_clause('"C"', USER_COLLATIONS), ' COLLATE "C"')

    def test_unknown_collation_is_dropped(self):
        connector = build_connector()
        self.assertEqual(connector.get_collate_clause('"de_DE.iso88591"', USER_COLLATIONS), '')

    def test_collation_of_another_engine_is_dropped(self):
        connector = build_connector(source_db_type='mysql')
        self.assertEqual(connector.get_collate_clause('utf8mb4_general_ci', {}), '')

    def test_collation_is_kept_when_target_cannot_be_queried(self):
        # An empty set means the target collations could not be read - dropping every
        # reference in that situation would silently change the semantics of the schema.
        connector = build_connector(existing_collations=set())
        self.assertEqual(connector.get_collate_clause('"en_US.utf8"', {}), ' COLLATE "en_US.utf8"')

    def test_index_sql_references_migrated_collation(self):
        connector = build_connector()
        sql = connector.get_create_index_sql({
            'index_name': 'countries_name_natural_idx',
            'index_type': 'INDEX',
            'target_schema_name': 'migtest',
            'target_table_name': 'countries',
            'index_columns': '"name" COLLATE "natural_numeric"',
            'user_collations': USER_COLLATIONS,
        })
        self.assertIn('COLLATE "migtest"."natural_numeric"', sql)

    def test_index_sql_keeps_operator_class_behind_collation(self):
        connector = build_connector()
        sql = connector.get_create_index_sql({
            'index_name': 'i1',
            'index_type': 'INDEX',
            'target_schema_name': 'migtest',
            'target_table_name': 't',
            'index_columns': '"a" COLLATE "natural_numeric" text_pattern_ops DESC NULLS LAST, "b"',
            'user_collations': USER_COLLATIONS,
        })
        self.assertIn('COLLATE "migtest"."natural_numeric" "text_pattern_ops" DESC NULLS LAST', sql)

    def test_function_based_index_keeps_collation(self):
        connector = build_connector()
        sql = connector.get_create_index_sql({
            'index_name': 'i2',
            'index_type': 'INDEX',
            'target_schema_name': 'migtest',
            'target_table_name': 't',
            'is_function_based': 'YES',
            'index_columns': '(lower("name") COLLATE "natural_numeric")',
            'user_collations': USER_COLLATIONS,
        })
        self.assertIn('COLLATE "migtest"."natural_numeric"', sql)

    def test_temporal_primary_key_uses_constraint_definition(self):
        connector = build_connector(names_case='keep')
        sql = connector.get_create_index_sql({
            'index_name': 'product_validity_pkey',
            'index_type': 'PRIMARY KEY',
            'target_schema_name': 'migtest',
            'target_table_name': 'product_validity',
            'index_columns': 'product_id, valid_at',
            'using_method': 'gist',
            'constraint_def': 'PRIMARY KEY (product_id, valid_at WITHOUT OVERLAPS)',
        })
        self.assertEqual(
            sql,
            'ALTER TABLE "migtest"."product_validity" ADD CONSTRAINT '
            '"product_validity_pkey_tab_product_validity" '
            'PRIMARY KEY (product_id, valid_at WITHOUT OVERLAPS);')
        # never a "CREATE UNIQUE INDEX ... USING gist", which is not a legal statement
        self.assertNotIn('CREATE', sql)

    def test_constraint_definition_wins_over_key_list(self):
        connector = build_connector(names_case='keep')
        sql = connector.get_create_index_sql({
            'index_name': 'orders_pkey',
            'index_type': 'PRIMARY KEY',
            'target_schema_name': 'migtest',
            'target_table_name': 'orders',
            'index_columns': 'order_id',
            'constraint_def': 'PRIMARY KEY (order_id) INCLUDE (status)',
        })
        self.assertIn('PRIMARY KEY (order_id) INCLUDE (status)', sql)

    def test_constraint_definition_follows_names_case_handling(self):
        # keywords are case folded together with the identifiers, as in the constraints
        # migration - PostgreSQL keywords are not case sensitive, so this stays valid SQL
        connector = build_connector(names_case='lower')
        sql = connector.get_create_index_sql({
            'index_name': 'product_validity_pkey',
            'index_type': 'PRIMARY KEY',
            'target_schema_name': 'migtest',
            'target_table_name': 'product_validity',
            'index_columns': 'product_id, valid_at',
            'constraint_def': 'PRIMARY KEY (product_id, valid_at WITHOUT OVERLAPS)',
        })
        self.assertIn('without overlaps', sql)

    def test_plain_primary_key_without_constraint_definition(self):
        # other source engines do not supply a constraint definition
        connector = build_connector(source_db_type='mysql')
        sql = connector.get_create_index_sql({
            'index_name': 'orders_pkey',
            'index_type': 'PRIMARY KEY',
            'target_schema_name': 'migtest',
            'target_table_name': 'orders',
            'index_columns': 'order_id',
        })
        self.assertIn('ADD CONSTRAINT "orders_pkey_tab_orders" PRIMARY KEY ("order_id")', sql)

    def test_extract_index_key_list(self):
        connector = build_connector()
        cases = [
            ('CREATE INDEX i ON public.t USING btree (lower(company_name))',
             'lower(company_name)'),
            ('CREATE UNIQUE INDEX i ON public.t USING btree (lower((email)::text))',
             'lower((email)::text)'),
            ("CREATE INDEX i ON public.t USING btree (((metadata ->> 'reference'::text)))",
             "((metadata ->> 'reference'::text))"),
            ('CREATE INDEX i ON public.t USING btree (((billing_address).city))',
             '((billing_address).city)'),
            ('CREATE INDEX i ON public.t USING btree (email) INCLUDE (company_name, is_active)',
             'email'),
            ('CREATE INDEX i ON public.t USING gist (resource_id, during) WHERE (NOT cancelled)',
             'resource_id, during'),
            ("CREATE INDEX i ON public.t USING brin (occurred_at) WITH (pages_per_range='32')",
             'occurred_at'),
            ('CREATE INDEX i ON public.t USING gin (event_type gin_trgm_ops, payload)',
             'event_type gin_trgm_ops, payload'),
            # a closing parenthesis inside a string literal must not end the key list
            ("CREATE INDEX i ON public.t USING btree (replace(name, ')'::text, ''::text))",
             "replace(name, ')'::text, ''::text)"),
            # definition without the USING clause
            ('CREATE INDEX i ON public.t (a, b)', 'a, b'),
        ]
        for index_sql, expected in cases:
            self.assertEqual(connector.extract_index_key_list(index_sql), expected, index_sql)

    def test_functional_index_expression_is_kept_whole(self):
        connector = build_connector()
        sql = connector.get_create_index_sql({
            'index_name': 'customers_lower_company_idx',
            'index_type': 'INDEX',
            'target_schema_name': 'migtest',
            'target_table_name': 'customers',
            'index_columns': 'lower(company_name)',
            'is_function_based': 'YES',
            'target_columns': {1: {'column_name': 'company_name', 'data_type': 'text'}},
        })
        self.assertIn('((lower("company_name")))', sql)
        self.assertNotIn('"lower(company_name"', sql)

    def test_functional_index_with_cast_and_mixed_keys(self):
        connector = build_connector()
        sql = connector.get_create_index_sql({
            'index_name': 'i1',
            'index_type': 'INDEX',
            'target_schema_name': 'migtest',
            'target_table_name': 'employees',
            'index_columns': 'department_id, lower((email)::text)',
            'is_function_based': 'YES',
            'target_columns': {
                1: {'column_name': 'department_id', 'data_type': 'integer'},
                2: {'column_name': 'email', 'data_type': 'varchar'},
            },
        })
        self.assertIn('"department_id"', sql)
        self.assertIn('(lower(("email")::text))', sql)

    def test_index_keeps_access_method_and_operator_class(self):
        connector = build_connector()
        sql = connector.get_create_index_sql({
            'index_name': 'customer_events_type_payload_idx',
            'index_type': 'INDEX',
            'target_schema_name': 'migtest',
            'target_table_name': 'customer_events',
            'index_columns': 'event_type gin_trgm_ops, payload',
            'using_method': 'gin',
        })
        self.assertIn('USING gin', sql)
        self.assertIn('"event_type" "gin_trgm_ops"', sql)

    def test_index_access_method_falls_back_to_source_ddl(self):
        connector = build_connector()
        sql = connector.get_create_index_sql({
            'index_name': 'customer_notes_body_gin_idx',
            'index_type': 'INDEX',
            'target_schema_name': 'migtest',
            'target_table_name': 'customer_notes',
            'index_columns': 'body_tsv',
            'source_index_sql': 'CREATE INDEX customer_notes_body_gin_idx ON public.customer_notes USING gin (body_tsv)',
        })
        self.assertIn('USING gin', sql)

    def test_index_access_method_of_other_engines_is_ignored(self):
        # MySQL "USING HASH" does not name a PostgreSQL access method
        connector = build_connector(source_db_type='mysql')
        sql = connector.get_create_index_sql({
            'index_name': 'i1',
            'index_type': 'INDEX',
            'target_schema_name': 'migtest',
            'target_table_name': 't',
            'index_columns': 'a',
            'source_index_sql': 'CREATE INDEX i1 ON t (a) USING HASH',
        })
        self.assertNotIn('USING', sql)

    def test_create_collation_sql_icu(self):
        connector = build_connector()
        sql = connector.get_create_collation_sql({
            'collation_name': 'natural_numeric',
            'target_schema_name': 'migtest',
            'collation_provider': 'icu',
            'collation_locale': 'en-u-kn',
            'collation_lc_collate': None,
            'collation_lc_ctype': None,
            'collation_deterministic': True,
            'collation_rules': None,
        })
        self.assertEqual(
            sql,
            'CREATE COLLATION IF NOT EXISTS "migtest"."natural_numeric" (provider = icu, locale = \'en-u-kn\');')

    def test_create_collation_sql_nondeterministic_with_rules(self):
        connector = build_connector()
        sql = connector.get_create_collation_sql({
            'collation_name': 'ci',
            'target_schema_name': 'migtest',
            'collation_provider': 'icu',
            'collation_locale': 'und-u-ks-level2',
            'collation_lc_collate': None,
            'collation_lc_ctype': None,
            'collation_deterministic': False,
            'collation_rules': '&V << w <<< W',
        })
        self.assertIn("rules = '&V << w <<< W'", sql)
        self.assertIn('deterministic = false', sql)

    def test_create_collation_sql_libc_split_locale(self):
        connector = build_connector()
        sql = connector.get_create_collation_sql({
            'collation_name': 'mixed',
            'target_schema_name': 'migtest',
            'collation_provider': 'libc',
            'collation_locale': None,
            'collation_lc_collate': 'de_DE.utf8',
            'collation_lc_ctype': 'C',
            'collation_deterministic': True,
            'collation_rules': None,
        })
        self.assertIn("lc_collate = 'de_DE.utf8'", sql)
        self.assertIn("lc_ctype = 'C'", sql)

    def test_create_collation_sql_without_locale_is_empty(self):
        connector = build_connector()
        sql = connector.get_create_collation_sql({
            'collation_name': 'broken',
            'target_schema_name': 'migtest',
            'collation_provider': 'default',
            'collation_locale': None,
            'collation_lc_collate': None,
            'collation_lc_ctype': None,
            'collation_deterministic': True,
            'collation_rules': None,
        })
        self.assertEqual(sql, '')

    def test_create_table_sql_emits_column_collation(self):
        connector = build_connector()
        connector.fetch_table_names = MagicMock(return_value={})
        mock_migrator_tables = MagicMock()
        mock_migrator_tables.get_domain_details.return_value = None
        sql = connector.get_create_table_sql({
            'source_schema_name': 'migtest',
            'source_table_name': 'countries',
            'source_table_id': 1,
            'target_schema_name': 'migtest',
            'target_table_name': 'countries',
            'migrator_tables': mock_migrator_tables,
            'user_collations': USER_COLLATIONS,
            'target_columns': {
                1: {
                    'column_name': 'name', 'data_type': 'TEXT', 'is_identity': 'NO',
                    'is_nullable': 'NO', 'column_default_name': '', 'column_default_value': '',
                    'replaced_column_default_value': '', 'character_maximum_length': '',
                    'basic_character_maximum_length': '', 'numeric_precision': '', 'numeric_scale': '',
                    'basic_numeric_precision': '', 'basic_numeric_scale': '',
                    'udt_schema': '', 'udt_name': '', 'domain_name': '', 'column_comment': '',
                    'is_generated_virtual': 'NO', 'is_generated_stored': 'NO',
                    'is_hidden_column': 'NO', 'collation_name': 'natural_numeric',
                },
            },
        })
        self.assertIn('"name" TEXT COLLATE "migtest"."natural_numeric" NOT NULL', sql)


if __name__ == '__main__':
    unittest.main()


class TestPgConstraintTriggers(unittest.TestCase):
    """A CONSTRAINT TRIGGER is registered in pg_constraint, but it is a trigger."""

    def _fetch_constraints(self, rows):
        connector = build_connector()
        connector.connect = MagicMock()
        connector.disconnect = MagicMock()
        cursor = MagicMock()
        cursor.fetchall.return_value = rows
        connector.connection = MagicMock()
        connector.connection.cursor.return_value = cursor
        return connector.fetch_constraints({
            'source_table_id': 1, 'source_table_schema': 'public',
            'source_table_name': 'orders_2025'})

    def test_constraint_trigger_is_left_to_the_triggers_migration(self):
        # (oid, conname, type, condef, ref_schema, ref_table, comment)
        rows = [
            (1, 'orders_must_have_items', 'TRIGGER', 'TRIGGER DEFERRABLE INITIALLY DEFERRED',
             None, '', 'fires at COMMIT'),
            (2, 'orders_shipped_after_placed', 'CHECK', 'CHECK ((shipped_at >= placed_at))',
             None, '', None),
        ]
        constraints = self._fetch_constraints(rows)
        names = {c['constraint_name'] for c in constraints.values()}
        self.assertNotIn('orders_must_have_items', names)
        self.assertIn('orders_shipped_after_placed', names)

    def test_primary_key_is_left_to_the_indexes_migration(self):
        rows = [
            (1, 'orders_pkey', 'PRIMARY KEY', 'PRIMARY KEY (order_id)', None, '', None),
            (2, 'orders_uq', 'UNIQUE', 'UNIQUE (reference)', None, '', None),
        ]
        constraints = self._fetch_constraints(rows)
        names = {c['constraint_name'] for c in constraints.values()}
        self.assertNotIn('orders_pkey', names)
        self.assertIn('orders_uq', names)


TEXT_SEARCH = {
    'migtest_english': {'target_schema_name': 'migtest', 'target_object_name': 'migtest_english',
                        'object_type': 'CONFIGURATION'},
    'mydict': {'target_schema_name': 'migtest', 'target_object_name': 'mydict',
               'object_type': 'DICTIONARY'},
}


class TestPgTextSearch(unittest.TestCase):

    def test_migrated_configuration_is_qualified(self):
        connector = build_connector()
        self.assertEqual(
            connector.qualify_text_search_references(
                "to_tsvector('migtest_english'::regconfig, body)", TEXT_SEARCH),
            "to_tsvector('migtest.migtest_english'::regconfig, body)")

    def test_qualified_source_reference_is_remapped(self):
        connector = build_connector()
        self.assertEqual(
            connector.qualify_text_search_references(
                "to_tsvector('public.migtest_english'::regconfig, body)", TEXT_SEARCH),
            "to_tsvector('migtest.migtest_english'::regconfig, body)")

    def test_builtin_and_extension_objects_are_untouched(self):
        connector = build_connector()
        for expression in ("to_tsvector('english'::regconfig, body)",
                           "to_tsvector('pg_catalog.english'::regconfig, body)",
                           "ts_lexize('ext.unaccent'::regdictionary, 'x')"):
            self.assertEqual(connector.qualify_text_search_references(expression, TEXT_SEARCH),
                             expression)

    def test_dictionary_reference_is_qualified(self):
        connector = build_connector()
        self.assertEqual(
            connector.qualify_text_search_references("ts_lexize('mydict'::regdictionary, 'x')", TEXT_SEARCH),
            "ts_lexize('migtest.mydict'::regdictionary, 'x')")

    def test_plain_string_without_cast_is_untouched(self):
        connector = build_connector()
        self.assertEqual(
            connector.qualify_text_search_references("SELECT 'migtest_english' AS name", TEXT_SEARCH),
            "SELECT 'migtest_english' AS name")

    def test_names_needing_quoting_are_quoted(self):
        connector = build_connector()
        objects = {'My Config': {'target_schema_name': 'My Schema',
                                 'target_object_name': 'My Config',
                                 'object_type': 'CONFIGURATION'}}
        self.assertEqual(
            connector.qualify_text_search_references("""to_tsvector('"My Config"'::regconfig, body)""", objects),
            """to_tsvector('"My Schema"."My Config"'::regconfig, body)""")

    def test_view_ddl_is_rewritten(self):
        connector = build_connector()
        ddl = connector.convert_view_code({
            'view_code': "SELECT plainto_tsquery('migtest_english'::regconfig, 'invoice') AS q",
            'target_view_name': 'v_note_search',
            'target_schema_name': 'migtest',
            'view_type': 'VIEW',
            'text_search_objects': TEXT_SEARCH,
        })
        self.assertIn("'migtest.migtest_english'::regconfig", ddl)

    def test_create_configuration_sql(self):
        connector = build_connector()
        sql = connector.get_create_text_search_sql({
            'object_name': 'migtest_english',
            'object_type': 'CONFIGURATION',
            'target_schema_name': 'migtest',
            'parser_name': 'pg_catalog."default"',
            'mappings': [('hword', ['ext.unaccent', 'pg_catalog.english_stem']),
                         ('word', ['ext.unaccent', 'pg_catalog.english_stem'])],
        })
        lines = sql.splitlines()
        self.assertEqual(lines[0],
                         'CREATE TEXT SEARCH CONFIGURATION "migtest"."migtest_english" '
                         '(PARSER = pg_catalog."default");')
        self.assertIn('ADD MAPPING FOR hword WITH ext.unaccent, pg_catalog.english_stem;', lines[1])
        self.assertEqual(len(lines), 3)

    def test_create_dictionary_sql(self):
        connector = build_connector()
        sql = connector.get_create_text_search_sql({
            'object_name': 'mydict',
            'object_type': 'DICTIONARY',
            'target_schema_name': 'migtest',
            'template_name': 'pg_catalog.snowball',
            'init_options': "language = 'english', stopwords = 'english'",
        })
        self.assertEqual(
            sql,
            'CREATE TEXT SEARCH DICTIONARY "migtest"."mydict" '
            "(TEMPLATE = pg_catalog.snowball, language = 'english', stopwords = 'english');")

    def test_configuration_without_parser_is_skipped(self):
        connector = build_connector()
        self.assertEqual(connector.get_create_text_search_sql({
            'object_name': 'broken', 'object_type': 'CONFIGURATION',
            'target_schema_name': 'migtest', 'parser_name': '', 'mappings': []}), '')


class TestPgIndexTail(unittest.TestCase):

    def test_extract_index_tail(self):
        connector = build_connector()
        cases = [
            ('CREATE INDEX i ON public.t USING btree (email) INCLUDE (a, b)', 'INCLUDE (a, b)'),
            ('CREATE INDEX i ON public.t USING btree (created_at DESC) WHERE is_active',
             'WHERE is_active'),
            ('CREATE INDEX i ON public.t USING gist (a, b) WHERE (NOT cancelled)',
             'WHERE (NOT cancelled)'),
            ("CREATE INDEX i ON public.t USING brin (a) WITH (pages_per_range='32', autosummarize='on')",
             "WITH (pages_per_range='32', autosummarize='on')"),
            ('CREATE UNIQUE INDEX i ON public.t USING btree (ean) NULLS NOT DISTINCT',
             'NULLS NOT DISTINCT'),
            ('CREATE INDEX i ON public.t USING btree (lower(name))', ''),
            # a tablespace of the source does not have to exist in the target
            ('CREATE INDEX i ON public.t USING btree (a) TABLESPACE fast WHERE a > 0',
             'WHERE a > 0'),
        ]
        for index_sql, expected in cases:
            self.assertEqual(connector.extract_index_tail(index_sql), expected, index_sql)

    def test_index_tail_is_emitted(self):
        connector = build_connector(names_case='keep')
        sql = connector.get_create_index_sql({
            'index_name': 'customers_active_idx',
            'index_type': 'INDEX',
            'target_schema_name': 'migtest',
            'target_table_name': 'customers',
            'index_columns': 'created_at DESC',
            'index_tail': 'WHERE is_active',
        })
        self.assertTrue(sql.endswith('("created_at" DESC) WHERE is_active;'), sql)

    def test_unique_index_keeps_uniqueness(self):
        connector = build_connector(names_case='keep')
        sql = connector.get_create_index_sql({
            'index_name': 'products_ean_uidx',
            'index_type': 'UNIQUE',
            'target_schema_name': 'migtest',
            'target_table_name': 'products',
            'index_columns': 'ean',
            'index_tail': 'NULLS NOT DISTINCT',
        })
        self.assertIn('CREATE UNIQUE INDEX', sql)
        self.assertIn('NULLS NOT DISTINCT', sql)

    def test_text_search_reference_in_partial_index_predicate(self):
        connector = build_connector(names_case='keep')
        sql = connector.get_create_index_sql({
            'index_name': 'i1',
            'index_type': 'INDEX',
            'target_schema_name': 'migtest',
            'target_table_name': 't',
            'index_columns': 'body',
            'index_tail': "WHERE (to_tsvector('migtest_english'::regconfig, body) @@ 'x'::tsquery)",
            'text_search_objects': TEXT_SEARCH,
        })
        self.assertIn("'migtest.migtest_english'::regconfig", sql)


class TestPgGeneratedColumns(unittest.TestCase):
    """A PostgreSQL source has to deliver the generation expression, not only the flag."""

    def _fetch_columns(self, rows):
        connector = build_connector()
        connector.connect = MagicMock()
        connector.disconnect = MagicMock()
        cursor = MagicMock()
        cursor.fetchall.return_value = rows
        connector.connection = MagicMock()
        connector.connection.cursor.return_value = cursor
        return connector.fetch_table_columns({'table_schema': 'public', 'table_name': 't'})

    def test_stored_generated_column(self):
        rows = [(1, 'full_name', 'text', None, None, None, 'NO', 'YES', None, 'pg_catalog',
                 'text', None, 'ALWAYS', 'text', None, None,
                 's', "((first_name || ' '::text) || last_name)")]
        column = self._fetch_columns(rows)[1]
        self.assertEqual(column['is_generated_stored'], 'YES')
        self.assertEqual(column['is_generated_virtual'], 'NO')
        self.assertEqual(column['stripped_generation_expression'],
                         "((first_name || ' '::text) || last_name)")

    def test_virtual_generated_column(self):
        rows = [(1, 'net', 'numeric', None, 14, 4, 'NO', 'YES', None, 'pg_catalog',
                 'numeric', None, 'ALWAYS', 'numeric(14,4)', None, None,
                 'v', '(gross / (1 + rate))')]
        column = self._fetch_columns(rows)[1]
        self.assertEqual(column['is_generated_virtual'], 'YES')
        self.assertEqual(column['is_generated_stored'], 'NO')
        self.assertEqual(column['generation_expression'], '(gross / (1 + rate))')

    def test_ordinary_column_is_not_generated(self):
        rows = [(1, 'body', 'text', None, None, None, 'NO', 'YES', None, 'pg_catalog',
                 'text', None, 'NEVER', 'text', None, None, '', None)]
        column = self._fetch_columns(rows)[1]
        self.assertEqual(column['is_generated_stored'], 'NO')
        self.assertEqual(column['is_generated_virtual'], 'NO')
        self.assertEqual(column['stripped_generation_expression'], '')

    def test_postgresql_expression_keeps_its_plus_operator(self):
        # the '+' -> '||' rewriting is meant for source engines concatenating with '+'
        connector = build_connector()
        self.assertEqual(
            connector.convert_generation_expression('(a + b)', {}, 'TEXT'),
            '(a + b)')


class TestPgGeneratedColumnsInDataMigration(unittest.TestCase):
    """A generated column is computed by the target and rejects an inserted value."""

    def _column(self, name, generated=None):
        return {'column_name': name, 'data_type': 'text',
                'is_generated_virtual': 'YES' if generated == 'virtual' else 'NO',
                'is_generated_stored': 'YES' if generated == 'stored' else 'NO'}

    def _run_migrate_table(self, source_columns, target_columns, rows):
        connector = build_connector(names_case='keep')
        connector.config_parser.get_total_chunks.return_value = 1
        connector.get_rows_count = MagicMock(return_value=len(rows))
        cursor = MagicMock()
        cursor.fetchmany.side_effect = [rows, []]
        connector.connection = MagicMock()
        connector.connection.cursor.return_value = cursor

        target_connection = MagicMock()
        target_connection.get_rows_count.return_value = 0
        target_connection.insert_batch.return_value = len(rows)

        migrator_tables = MagicMock()
        migrator_tables.insert_data_migration.return_value = 1
        migrator_tables.select_primary_key.return_value = ''

        connector.migrate_table(target_connection, {
            'worker_id': 'test', 'source_schema_name': 'public', 'source_table_name': 't',
            'source_table_id': 1, 'source_columns': source_columns,
            'target_schema_name': 'migtest', 'target_table_name': 't',
            'target_columns': target_columns, 'batch_size': 10,
            'migrator_tables': migrator_tables, 'migration_limitation': None,
            'chunk_size': -1, 'chunk_number': 1, 'resume_after_crash': False,
            'drop_unfinished_tables': False, 'source_table_rows_all': len(rows),
        })
        return cursor.execute.call_args[0][0], target_connection.insert_batch.call_args[0][0]

    def test_generated_column_is_excluded_from_select_and_insert(self):
        columns = {'1': self._column('request_id'), '2': self._column('endpoint'),
                   '3': self._column('request_time', generated='virtual')}
        query, insert_settings = self._run_migrate_table(columns, columns, [('a', 'b', 'c')])
        self.assertIn('"request_id", "endpoint"', query)
        self.assertNotIn('request_time', query)
        self.assertNotIn('request_time', insert_settings['insert_columns'])
        # the record must stay aligned with the reduced SELECT
        self.assertEqual(insert_settings['data'], [{'request_id': 'a', 'endpoint': 'b'}])

    def test_stored_generated_column_is_excluded_too(self):
        columns = {'1': self._column('quantity'), '2': self._column('line_total', generated='stored')}
        query, insert_settings = self._run_migrate_table(columns, columns, [('3', '9')])
        self.assertNotIn('line_total', query)
        self.assertEqual(insert_settings['data'], [{'quantity': '3'}])

    def test_generated_only_on_the_target_side_is_excluded(self):
        source_columns = {'1': self._column('a'), '2': self._column('b')}
        target_columns = {'1': self._column('a'), '2': self._column('b', generated='stored')}
        query, insert_settings = self._run_migrate_table(source_columns, target_columns, [('1', '2')])
        self.assertNotIn('"b"', query)
        self.assertEqual(insert_settings['data'], [{'a': '1'}])

    def test_ordinary_columns_are_all_migrated(self):
        columns = {'1': self._column('a'), '2': self._column('b')}
        query, insert_settings = self._run_migrate_table(columns, columns, [('1', '2')])
        self.assertIn('"a", "b"', query)
        self.assertEqual(insert_settings['insert_columns'], '"a", "b"')
        self.assertEqual(insert_settings['data'], [{'a': '1', 'b': '2'}])


class TestStripGeneratedColumnClauses(unittest.TestCase):
    """The staging table of the LOB import must accept the values from the data file."""

    def test_stored_and_virtual_clauses_are_removed(self):
        connector = build_connector()
        self.assertEqual(
            connector.strip_generated_column_clauses(
                'CREATE TABLE "s"."t" ("b" TEXT GENERATED ALWAYS AS (("x" || \' \'::text) || "y") STORED, "c" TEXT)'),
            'CREATE TABLE "s"."t" ("b" TEXT, "c" TEXT)')
        self.assertEqual(
            connector.strip_generated_column_clauses(
                'CREATE TABLE "s"."t" ("n" NUMERIC GENERATED ALWAYS AS ("g" / ((1)::numeric + "r")) VIRTUAL, "m" INT)'),
            'CREATE TABLE "s"."t" ("n" NUMERIC, "m" INT)')

    def test_nested_parentheses_and_literals_survive(self):
        connector = build_connector()
        self.assertEqual(
            connector.strip_generated_column_clauses(
                'CREATE TABLE "s"."t" ("p" TEXT GENERATED ALWAYS AS '
                '(CASE WHEN ("c" = (0)::numeric) THEN NULL::numeric ELSE round((("l" - "c") / "c"), 2) END) STORED, "q" INT)'),
            'CREATE TABLE "s"."t" ("p" TEXT, "q" INT)')

    def test_identity_columns_are_not_touched(self):
        connector = build_connector()
        for sql in ('CREATE TABLE "s"."t" ("a" INT GENERATED BY DEFAULT AS IDENTITY, "b" INT)',
                    'CREATE TABLE "s"."t" ("a" INT GENERATED ALWAYS AS IDENTITY, "b" INT)'):
            self.assertEqual(connector.strip_generated_column_clauses(sql), sql)

    def test_table_without_generated_columns_is_unchanged(self):
        connector = build_connector()
        sql = 'CREATE TABLE "s"."t" ("a" INT, "b" INT)'
        self.assertEqual(connector.strip_generated_column_clauses(sql), sql)

    def test_empty_input(self):
        connector = build_connector()
        self.assertEqual(connector.strip_generated_column_clauses(''), '')
        self.assertIsNone(connector.strip_generated_column_clauses(None))


class TestPgIndexTypeDerivation(unittest.TestCase):
    """The index type must not depend on information_schema visibility rules."""

    def _fetch(self, rows):
        connector = build_connector()
        connector.connect = MagicMock()
        connector.disconnect = MagicMock()
        cursor = MagicMock()
        cursor.fetchall.return_value = rows
        connector.connection = MagicMock()
        connector.connection.cursor.return_value = cursor
        result = connector.fetch_indexes({
            'source_table_id': 1, 'source_table_schema': 'public',
            'source_table_name': 't'})
        return result, cursor.execute.call_args[0][0]

    def _row(self, name, indexdef, label, is_unique, contype, condef):
        # indexname, indexdef, type, comment, is_expression, is_unique, contype, condef
        return (name, indexdef, label, None, False, is_unique, contype, condef)

    def test_query_reads_the_type_from_pg_constraint_only(self):
        _, query = self._fetch([])
        self.assertNotIn('information_schema', query)
        self.assertIn("CASE con.contype", query)
        self.assertIn("WHEN 'p' THEN 'PRIMARY KEY'", query)
        self.assertIn("WHEN 'u' THEN 'UNIQUE'", query)

    def test_primary_key_of_a_non_identity_column(self):
        indexes, _ = self._fetch([self._row(
            'fk_parent_pkey', 'CREATE UNIQUE INDEX fk_parent_pkey ON public.fk_parent USING btree (parent_id)',
            'PRIMARY KEY', True, 'p', 'PRIMARY KEY (parent_id)')])
        self.assertEqual(len(indexes), 1)
        self.assertEqual(indexes[1]['index_type'], 'PRIMARY KEY')
        self.assertEqual(indexes[1]['index_columns'], 'parent_id')
        self.assertEqual(indexes[1]['constraint_def'], 'PRIMARY KEY (parent_id)')

    def test_unique_constraint_is_left_to_the_constraints_migration(self):
        indexes, _ = self._fetch([self._row(
            'fk_parent_alt_key', 'CREATE UNIQUE INDEX fk_parent_alt_key ON public.fk_parent USING btree (alt_key_a, alt_key_b)',
            'UNIQUE', True, 'u', 'UNIQUE (alt_key_a, alt_key_b)')])
        self.assertEqual(indexes, {})

    def test_exclusion_constraint_is_left_to_the_constraints_migration(self):
        # information_schema.table_constraints does not list an exclusion constraint at all
        indexes, _ = self._fetch([self._row(
            'no_overlap', 'CREATE INDEX no_overlap ON public.t USING gist (a, b)',
            'EXCLUSION', False, 'x', 'EXCLUDE USING gist (a WITH =, b WITH &&)')])
        self.assertEqual(indexes, {})

    def test_plain_unique_index_keeps_its_uniqueness(self):
        indexes, _ = self._fetch([self._row(
            'products_ean_uidx', 'CREATE UNIQUE INDEX products_ean_uidx ON public.products USING btree (ean)',
            'INDEX', True, None, None)])
        self.assertEqual(indexes[1]['index_type'], 'UNIQUE')
        self.assertEqual(indexes[1]['constraint_def'], '')

    def test_ordinary_index_stays_an_index(self):
        indexes, _ = self._fetch([self._row(
            'i1', 'CREATE INDEX i1 ON public.t USING btree (a)', 'INDEX', False, None, None)])
        self.assertEqual(indexes[1]['index_type'], 'INDEX')
