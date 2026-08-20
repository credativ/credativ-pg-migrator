import unittest

from credativ_pg_migrator.anonymization.registry import anonymization_registry
from credativ_pg_migrator.anonymization.routing import (
    AnonymizationConfigError,
    AnonymizationValueTooLongError,
    MigratorAnonymizer,
)


def make_config(policy=None, attempts=None, rules=None):
    anonymization = {'tables': rules if rules is not None else {'customers': {'note': {'method': 'faker_name'}}}}
    if policy is not None:
        anonymization['on_value_too_long'] = policy
    if attempts is not None:
        anonymization['find_fitting_value_attempts'] = attempts
    return {'anonymization': anonymization}


class TestValueTooLongPolicy(unittest.TestCase):
    """
    A value which does not fit into the target column must never be cut without a trace - the
    migration modifies data and reports a clean run.
    """

    def setUp(self):
        # a method producing values of a decreasing, predictable length
        self.produced = []

        @anonymization_registry.register('test_shrinking_value')
        def shrinking_value(value, params):
            result = 'y' * (10 - len(self.produced))
            self.produced.append(result)
            return result

        @anonymization_registry.register('test_constant_long_value')
        def constant_long_value(value, params):
            return 'z' * 10

    def tearDown(self):
        anonymization_registry._methods.pop('test_shrinking_value', None)
        anonymization_registry._methods.pop('test_constant_long_value', None)

    def test_default_policy_is_error(self):
        anonymizer = MigratorAnonymizer(make_config())
        self.assertEqual(anonymizer.on_value_too_long, 'error')

    def test_unknown_policy_is_fatal(self):
        with self.assertRaises(AnonymizationConfigError) as ctx:
            MigratorAnonymizer(make_config(policy='truncate'))
        self.assertIn('on_value_too_long', str(ctx.exception))

    def test_invalid_attempts_are_fatal(self):
        with self.assertRaises(AnonymizationConfigError):
            MigratorAnonymizer(make_config(policy='find_fitting_value', attempts=0))
        with self.assertRaises(AnonymizationConfigError):
            MigratorAnonymizer(make_config(policy='find_fitting_value', attempts='many'))

    def test_error_policy_raises_for_an_anonymized_column(self):
        config = make_config(rules={'customers': {'note': {'method': 'test_constant_long_value'}}})
        anonymizer = MigratorAnonymizer(config)
        with self.assertRaises(AnonymizationValueTooLongError) as ctx:
            anonymizer.anonymize_row('customers', {'note': 'short'}, max_lengths={'note': 5})
        # the value itself is personal data and must not appear in the message
        self.assertNotIn('zzzzzzzzzz', str(ctx.exception))
        self.assertIn('note', str(ctx.exception))

    def test_error_policy_raises_for_a_column_copied_unchanged(self):
        anonymizer = MigratorAnonymizer(make_config())
        with self.assertRaises(AnonymizationValueTooLongError):
            anonymizer.anonymize_row('customers', {'city': 'a very long city name'}, max_lengths={'city': 5})

    def test_fit_policy_cuts_the_value_and_counts_it(self):
        config = make_config(policy='fit', rules={'customers': {'note': {'method': 'test_constant_long_value'}}})
        anonymizer = MigratorAnonymizer(config)
        length_stats = {}

        row = anonymizer.anonymize_row('customers', {'note': 'short', 'city': 'a very long city name'},
                                       max_lengths={'note': 5, 'city': 4}, length_stats=length_stats)

        self.assertEqual(row['note'], 'zzzzz')
        self.assertEqual(row['city'], 'a ve')
        self.assertEqual(length_stats['note']['truncated'], 1)
        self.assertEqual(length_stats['city']['truncated'], 1)

    def test_find_fitting_value_repeats_until_the_value_fits(self):
        config = make_config(policy='find_fitting_value',
                             rules={'customers': {'note': {'method': 'test_shrinking_value'}}})
        anonymizer = MigratorAnonymizer(config)
        length_stats = {}

        # the method returns 10, 9, 8 ... characters - 8 is the first one fitting into 8
        row = anonymizer.anonymize_row('customers', {'note': 'x'}, max_lengths={'note': 8},
                                       length_stats=length_stats)

        self.assertEqual(row['note'], 'y' * 8)
        self.assertEqual(length_stats['note']['refitted'], 1)
        self.assertEqual(length_stats['note']['truncated'], 0)

    def test_find_fitting_value_raises_when_the_method_is_deterministic(self):
        config = make_config(policy='find_fitting_value',
                             rules={'customers': {'note': {'method': 'test_constant_long_value'}}})
        anonymizer = MigratorAnonymizer(config)
        with self.assertRaises(AnonymizationValueTooLongError) as ctx:
            anonymizer.anonymize_row('customers', {'note': 'x'}, max_lengths={'note': 5})
        self.assertIn('same value', str(ctx.exception))

    def test_find_fitting_value_raises_after_the_configured_attempts(self):
        config = make_config(policy='find_fitting_value', attempts=2,
                             rules={'customers': {'note': {'method': 'test_shrinking_value'}}})
        anonymizer = MigratorAnonymizer(config)
        with self.assertRaises(AnonymizationValueTooLongError) as ctx:
            anonymizer.anonymize_row('customers', {'note': 'x'}, max_lengths={'note': 3})
        self.assertIn('2 attempts', str(ctx.exception))

    def test_find_fitting_value_raises_for_a_column_without_a_rule(self):
        anonymizer = MigratorAnonymizer(make_config(policy='find_fitting_value'))
        with self.assertRaises(AnonymizationValueTooLongError) as ctx:
            anonymizer.anonymize_row('customers', {'city': 'a very long city name'}, max_lengths={'city': 5})
        self.assertIn('no anonymization rule', str(ctx.exception))

    def test_values_which_fit_are_untouched(self):
        config = make_config(policy='fit', rules={'customers': {'note': {'method': 'test_constant_long_value'}}})
        anonymizer = MigratorAnonymizer(config)
        length_stats = {}

        row = anonymizer.anonymize_row('customers', {'note': 'x', 'city': 'Berlin', 'id': 7},
                                       max_lengths={'note': 10, 'city': 10}, length_stats=length_stats)

        self.assertEqual(row['note'], 'z' * 10)
        self.assertEqual(row['city'], 'Berlin')
        self.assertEqual(row['id'], 7)
        self.assertEqual(length_stats, {})

    def test_raw_sql_value_is_not_measured(self):
        # the value is a function call executed by the target - its length says nothing about
        # the length of the value the target will store
        config = {
            'anonymization': {
                'on_value_too_long': 'error',
                'tables': {'customers': {'city': {'method': 'postgres_anon_native',
                                                  'params': {'func_name': 'anon.fake_city'}}}},
            }
        }
        anonymizer = MigratorAnonymizer(config)
        row = anonymizer.anonymize_row('customers', {'city': 'Berlin'}, max_lengths={'city': 5})
        self.assertEqual(row['city'], '__RAW_SQL__:anon.fake_city()')

    def test_null_values_are_not_measured(self):
        anonymizer = MigratorAnonymizer(make_config(policy='error'))
        row = anonymizer.anonymize_row('customers', {'city': None}, max_lengths={'city': 1})
        self.assertIsNone(row['city'])


if __name__ == '__main__':
    unittest.main()
