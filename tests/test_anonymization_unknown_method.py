import unittest

from credativ_pg_migrator.anonymization.registry import anonymization_registry
from credativ_pg_migrator.anonymization.routing import AnonymizationConfigError, MigratorAnonymizer


class TestAnonymizationUnknownMethod(unittest.TestCase):
    """
    An unknown method must never end as a skipped column - the original personal data would be
    copied to a target everybody treats as anonymized.
    """

    def test_unknown_method_in_table_rule_is_fatal(self):
        config = {
            'anonymization': {
                'tables': {
                    'customers': {
                        'email': {'method': 'faker_emial'},   # typo
                    }
                }
            }
        }
        with self.assertRaises(AnonymizationConfigError) as ctx:
            MigratorAnonymizer(config)
        self.assertIn('faker_emial', str(ctx.exception))
        self.assertIn('anonymization.tables.customers.email', str(ctx.exception))

    def test_unknown_method_in_regex_mapping_is_fatal(self):
        config = {
            'anonymization': {
                'regex_mappings': [
                    {'table_pattern': '.*', 'column_pattern': '^email$', 'method': 'no_such_method'},
                ]
            }
        }
        with self.assertRaises(AnonymizationConfigError) as ctx:
            MigratorAnonymizer(config)
        self.assertIn('no_such_method', str(ctx.exception))

    def test_missing_method_is_fatal(self):
        config = {
            'anonymization': {
                'tables': {
                    'customers': {
                        'email': {'params': {'salt': 'x'}},
                    }
                }
            }
        }
        with self.assertRaises(AnonymizationConfigError):
            MigratorAnonymizer(config)

    def test_invalid_regex_pattern_is_fatal(self):
        config = {
            'anonymization': {
                'regex_mappings': [
                    {'table_pattern': '(unclosed', 'column_pattern': '.*', 'method': 'faker_email'},
                ]
            }
        }
        with self.assertRaises(AnonymizationConfigError):
            MigratorAnonymizer(config)

    def test_all_problems_are_reported_at_once(self):
        config = {
            'anonymization': {
                'tables': {
                    'customers': {
                        'email': {'method': 'faker_emial'},
                        'name': {'method': 'faker_nmae'},
                    }
                }
            }
        }
        with self.assertRaises(AnonymizationConfigError) as ctx:
            MigratorAnonymizer(config)
        self.assertIn('faker_emial', str(ctx.exception))
        self.assertIn('faker_nmae', str(ctx.exception))

    def test_valid_config_is_accepted_and_counted(self):
        config = {
            'anonymization': {
                'tables': {
                    'customers': {
                        'external_ref': {'method': 'deterministic_hash_mask', 'params': {'salt': 's'}},
                    }
                },
                'regex_mappings': [
                    {'table_pattern': '.*', 'column_pattern': '^iban$', 'method': 'static_mask'},
                ]
            }
        }
        anonymizer = MigratorAnonymizer(config)
        self.assertTrue(anonymizer.is_active())
        self.assertEqual(anonymizer.rules_count, 2)

    def test_anonymize_row_raises_when_method_disappears(self):
        config = {
            'anonymization': {
                'tables': {
                    'customers': {
                        'ssn': {'method': 'static_mask', 'params': {'mask_char': 'X'}},
                    }
                }
            }
        }
        anonymizer = MigratorAnonymizer(config)

        # simulate a method which is not resolvable at runtime
        removed = anonymization_registry._methods.pop('static_mask')
        try:
            with self.assertRaises(AnonymizationConfigError):
                anonymizer.anonymize_row('customers', {'ssn': '123-45-6789'})
        finally:
            anonymization_registry._methods['static_mask'] = removed

    def test_anonymize_row_replaces_values_and_counts_them(self):
        config = {
            'anonymization': {
                'tables': {
                    'customers': {
                        'ssn': {'method': 'static_mask', 'params': {'mask_char': 'X'}},
                    }
                }
            }
        }
        anonymizer = MigratorAnonymizer(config)
        stats = {}

        row = anonymizer.anonymize_row('customers', {'ssn': '12345', 'city': 'Berlin'}, stats=stats)
        self.assertEqual(row['ssn'], 'XXXXX')
        self.assertEqual(row['city'], 'Berlin')
        self.assertEqual(stats, {('ssn', 'static_mask'): 1})

        # NULL values carry no personal data and are not counted
        anonymizer.anonymize_row('customers', {'ssn': None}, stats=stats)
        self.assertEqual(stats, {('ssn', 'static_mask'): 1})

    def test_rules_for_columns_reports_matching_rules(self):
        config = {
            'anonymization': {
                'tables': {
                    'customers': {
                        'ssn': {'method': 'static_mask'},
                    }
                }
            }
        }
        anonymizer = MigratorAnonymizer(config)
        rules = anonymizer.get_rules_for_columns('customers', ['id', 'ssn', 'city'])
        self.assertEqual(list(rules.keys()), ['ssn'])
        self.assertEqual(rules['ssn'][0], 'static_mask')


if __name__ == '__main__':
    unittest.main()
