import os
import tempfile
import unittest

from credativ_pg_migrator.config_parser import ConfigParser


def make_config_parser():
    config_parser = ConfigParser.__new__(ConfigParser)
    config_parser.print_log_message = lambda level, message: None
    return config_parser


def columns(*name_and_type):
    return {index + 1: {'column_name': name, 'data_type': data_type.split('(')[0],
                        'numeric_scale': None}
            for index, (name, data_type) in enumerate(name_and_type)}


class TestDb2TemporalValues(unittest.TestCase):
    """
    A date, a time or a timestamp of a Db2 export is written the way Db2 writes it, and
    PostgreSQL reads none of those notations: 'invalid input syntax for type timestamp:
    "01/04/22-06.00.00"'.
    """

    def setUp(self):
        self.config_parser = make_config_parser()

    def convert(self, value, kind=None, date_order=None):
        return self.config_parser.convert_temporal_value(value, kind, date_order)

    def test_db2_timestamp_is_converted_in_any_column(self):
        ## the shape Db2 always writes for a four digit year - unmistakable, so it is
        ## converted even when the type of the column is not known
        self.assertEqual(self.convert('2026-06-01-06.01.00.000000'),
                         ('2026-06-01 06:01:00.000000', 'converted'))
        self.assertEqual(self.convert('2026-07-15-09.00.00', 'TIMESTAMP'),
                         ('2026-07-15 09:00:00', 'converted'))

    def test_fraction_longer_than_postgresql_stores_is_passed_on(self):
        ## TIMESTAMP(9) and TIMESTAMP(12) exist on Db2 - the target rounds to microseconds
        self.assertEqual(self.convert('2026-07-14-21.00.00.000007919', 'TIMESTAMP'),
                         ('2026-07-14 21:00:00.000007919', 'converted'))

    def test_timestamp_with_time_zone_of_zos(self):
        self.assertEqual(self.convert('2022-01-11-12.00.00.000000 +01:00', 'TIMESTAMP'),
                         ('2022-01-11 12:00:00.000000+01:00', 'converted'))
        self.assertEqual(self.convert('2022-06-11-12.00.00.000000-0500', 'TIMESTAMP'),
                         ('2022-06-11 12:00:00.000000-05:00', 'converted'))

    def test_date_formats_of_db2_for_i(self):
        self.assertEqual(self.convert('01/04/22-06.00.00', 'TIMESTAMP', 'MDY'),
                         ('2022-01-04 06:00:00', 'converted'))
        self.assertEqual(self.convert('01/04/22-06.00.00', 'TIMESTAMP', 'DMY'),
                         ('2022-04-01 06:00:00', 'converted'))
        self.assertEqual(self.convert('01.04.2022', 'DATE', 'DMY'), ('2022-04-01', 'converted'))
        self.assertEqual(self.convert('20220104', 'DATE', 'YMD'), ('2022-01-04', 'converted'))
        ## *JUL - a year and the day inside it, the same date in every order
        self.assertEqual(self.convert('22/123', 'DATE'), ('2022-05-03', 'converted'))

    def test_two_digit_year_uses_the_window_of_db2_for_i(self):
        self.assertEqual(self.convert('12/31/39', 'DATE', 'MDY'), ('2039-12-31', 'converted'))
        self.assertEqual(self.convert('01/01/40', 'DATE', 'MDY'), ('1940-01-01', 'converted'))

    def test_time_formats(self):
        self.assertEqual(self.convert('06.00.00', 'TIME'), ('06:00:00', 'converted'))
        self.assertEqual(self.convert('060000', 'TIME'), ('06:00:00', 'converted'))
        self.assertEqual(self.convert('06:00 PM', 'TIME'), ('18:00:00', 'converted'))
        self.assertEqual(self.convert('12:00 AM', 'TIME'), ('00:00:00', 'converted'))
        self.assertEqual(self.convert('23:59:59', 'TIME'), ('23:59:59', 'unchanged'))

    def test_a_date_which_needs_the_order_is_reported_and_not_guessed(self):
        self.assertEqual(self.convert('01/04/22', 'DATE'), ('01/04/22', 'ambiguous'))
        ## one which only one order can write does not need it
        self.assertEqual(self.convert('01/13/22', 'DATE'), ('2022-01-13', 'converted'))

    def test_text_columns_are_left_alone(self):
        ## the same value in a column whose type is not a date must not be touched
        self.assertEqual(self.convert('01/04/22'), ('01/04/22', 'unchanged'))
        self.assertEqual(self.convert('1.7000'), ('1.7000', 'unchanged'))
        self.assertEqual(self.convert('+49   '), ('+49   ', 'unchanged'))

    def test_a_value_which_is_no_date_is_migrated_as_it_was_exported(self):
        self.assertEqual(self.convert('OUT', 'TIMESTAMP'), ('OUT', 'unparsable'))
        self.assertEqual(self.convert('', 'DATE'), ('', 'unchanged'))
        self.assertEqual(self.convert(None, 'DATE'), (None, 'unchanged'))

    def test_unknown_date_format_of_the_configuration_is_fatal(self):
        with self.assertRaises(ValueError) as context:
            self.config_parser.date_format_to_order('TAGMONAT')
        self.assertIn('TAGMONAT', str(context.exception))
        self.assertIsNone(self.config_parser.date_format_to_order(None))
        self.assertEqual(self.config_parser.date_format_to_order('*USA'), 'MDY')
        self.assertEqual(self.config_parser.date_format_to_order('eur'), 'DMY')


class TestDb2CsvConversion(unittest.TestCase):
    """The conversion of the whole file, including the order the dates are written in."""

    def setUp(self):
        self.config_parser = make_config_parser()
        self.directory = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.directory.cleanup()

    def convert_file(self, content, source_columns, date_format=None, header=False):
        input_file = os.path.join(self.directory.name, 'data.csv')
        with open(input_file, 'w', encoding='utf-8') as file:
            file.write(content)
        settings = {
            'file_name': input_file,
            'converted_file_name': os.path.join(self.directory.name, 'converted.csv'),
            'source_table_name': 'TEST_TABLE',
            'file_size': os.path.getsize(input_file),
            'format_options': {'format': 'CSV', 'delimiter': ',', 'header': header,
                               'character_set': 'UTF-8', 'date_format': date_format},
        }
        self.config_parser.convert_csv_to_utf8(settings, source_columns, None)
        with open(settings['converted_file_name'], encoding='utf-8') as file:
            return [line.strip() for line in file if line.strip()]

    def test_the_order_is_worked_out_from_the_whole_file(self):
        ## the second row can only be MDY - which decides the first one as well
        rows = self.convert_file('1,"01/04/22-06.00.00"\n2,"05/25/22-06.00.00"\n',
                                 columns(('ID', 'INTEGER'), ('MOVED', 'TIMESTAMP(0)')))
        self.assertEqual(rows, ['1,2022-01-04 06:00:00', '2,2022-05-25 06:00:00'])

    def test_a_column_fitting_several_orders_stops_the_table(self):
        with self.assertRaises(ValueError) as context:
            self.convert_file('1,"01/04/22"\n2,"02/05/22"\n',
                              columns(('ID', 'INTEGER'), ('BOOKED', 'DATE')))
        message = str(context.exception)
        self.assertIn('BOOKED', message)
        self.assertIn('date_format', message)

    def test_the_configured_order_is_used_without_reading_the_file_twice(self):
        rows = self.convert_file('1,"01/04/22"\n2,"02/05/22"\n',
                                 columns(('ID', 'INTEGER'), ('BOOKED', 'DATE')),
                                 date_format='*DMY')
        self.assertEqual(rows, ['1,2022-04-01', '2,2022-05-02'])

    def test_values_of_two_different_orders_in_one_column_stop_the_table(self):
        with self.assertRaises(ValueError) as context:
            self.convert_file('1,"25/04/22"\n2,"04/25/22"\n',
                              columns(('ID', 'INTEGER'), ('BOOKED', 'DATE')))
        self.assertIn('no order of the parts fits every value', str(context.exception))

    def test_a_row_of_another_width_keeps_the_old_behaviour(self):
        ## the types cannot be assigned to the fields - only the Db2 timestamp is converted
        rows = self.convert_file('1,"01/04/22","2022-01-11-12.00.00"\n',
                                 columns(('ID', 'INTEGER'), ('BOOKED', 'DATE')))
        self.assertEqual(rows, ['1,01/04/22,2022-01-11 12:00:00'])

    def test_a_header_line_is_not_read_as_a_value(self):
        rows = self.convert_file('ID,BOOKED\n1,"01/13/22"\n',
                                 columns(('ID', 'INTEGER'), ('BOOKED', 'DATE')), header=True)
        self.assertEqual(rows, ['ID,BOOKED', '1,2022-01-13'])


if __name__ == '__main__':
    unittest.main()
