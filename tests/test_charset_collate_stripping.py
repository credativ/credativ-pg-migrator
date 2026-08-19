import unittest
from credativ_pg_migrator.connectors.mysql_connector import MySQLConnector
from credativ_pg_migrator.config_parser import ConfigParser

class TestCharsetCollateStripping(unittest.TestCase):
    def test_strip_character_set_and_collate_in_view(self):
        connector = MySQLConnector.__new__(MySQLConnector)
        config_parser = ConfigParser.__new__(ConfigParser)
        config_parser.config = {'target': {'type': 'postgresql'}}
        connector.config_parser = config_parser

        view_sql = "select `p`.`product_id` AS `product_id`,`p`.`sku` AS `sku`,`jt`.`tag` AS `tag` from (`migtest`.`products` `p` join json_table(`p`.`attributes`, '$.tags[*]' columns (`tag_idx` for ordinality, `tag` varchar(32) character set utf8mb4 path '$')) `jt`)"
        settings = {
            'view_code': view_sql,
            'source_schema_name': 'migtest',
            'target_schema_name': 'public',
            'target_db_type': 'postgresql'
        }

        res = connector.convert_view_code(settings)
        self.assertNotIn('character set', res.lower())
        self.assertNotIn('utf8mb4', res.lower())
        self.assertIn('varchar(32)', res.lower())

    def test_strip_collate_in_apply_sql_functions_mapping(self):
        connector = MySQLConnector.__new__(MySQLConnector)
        connector.config_parser = None

        code = "SELECT col VARCHAR(50) CHARACTER SET latin1 COLLATE latin1_swedish_ci"
        settings = {'target_db_type': 'postgresql'}

        res = connector.apply_sql_functions_mapping(code, settings)
        self.assertNotIn('CHARACTER SET', res)
        self.assertNotIn('latin1_swedish_ci', res)

    def test_with_rollup_conversion(self):
        connector = MySQLConnector.__new__(MySQLConnector)
        config_parser = ConfigParser.__new__(ConfigParser)
        config_parser.config = {'target': {'type': 'postgresql'}}
        connector.config_parser = config_parser

        view_sql = "select year(`o`.`order_date`) AS `sale_year`, `o`.`currency_code` AS `currency`, count(0) AS `order_count` from `migtest`.`orders` `o` group by year(`o`.`order_date`), `o`.`currency_code` with rollup"
        settings = {
            'view_code': view_sql,
            'source_schema_name': 'migtest',
            'target_schema_name': 'public',
            'target_db_type': 'postgresql'
        }

        res = connector.convert_view_code(settings)
        self.assertNotIn('WITH ROLLUP', res.upper())
        self.assertIn('GROUP BY ROLLUP', res.upper())

    def test_find_in_set_conversion(self):
        connector = MySQLConnector.__new__(MySQLConnector)
        connector.config_parser = None

        code = "SELECT * FROM customers WHERE FIND_IN_SET('SMS', marketing_channels) > 0"
        settings = {'target_db_type': 'postgresql'}

        res = connector.apply_sql_functions_mapping(code, settings)
        self.assertNotIn('FIND_IN_SET', res)
        self.assertIn("string_to_array(marketing_channels, ',')", res)
        self.assertIn("array_position", res)
        self.assertIn("coalesce", res)

    def test_date_extract_functions_conversion(self):
        connector = MySQLConnector.__new__(MySQLConnector)
        connector.config_parser = None

        code = 'SELECT YEAR(CAST("o"."order_date" AS DATE)), MONTH("order_date"), DAY("order_date") FROM orders'
        settings = {'target_db_type': 'postgresql'}

        res = connector.apply_sql_functions_mapping(code, settings)
        self.assertIn('EXTRACT(YEAR FROM CAST("o"."order_date" AS DATE))', res)
        self.assertIn('EXTRACT(MONTH FROM "order_date")', res)
        self.assertIn('EXTRACT(DAY FROM "order_date")', res)

    def test_mysql_internal_rollup_functions_conversion(self):
        connector = MySQLConnector.__new__(MySQLConnector)
        connector.config_parser = None

        code = 'SELECT ROLLUP_GROUP_ITEM("col", 0), ROLLUP_SUM_SWITCHER(SUM("total")) FROM orders GROUP BY ROLLUP ("col")'
        settings = {'target_db_type': 'postgresql'}

        res = connector.apply_sql_functions_mapping(code, settings)
        self.assertNotIn('ROLLUP_GROUP_ITEM', res)
        self.assertNotIn('ROLLUP_SUM_SWITCHER', res)
        self.assertIn('"col"', res)
        self.assertIn('SUM("total")', res)

    def test_char_cast_and_grouping_boolean_conversion(self):
        connector = MySQLConnector.__new__(MySQLConnector)
        connector.config_parser = None

        code = 'SELECT CAST(name AS CHAR(500)), CASE WHEN GROUPING(col) THEN "ALL" END FROM t'
        settings = {'target_db_type': 'postgresql'}

        res = connector.apply_sql_functions_mapping(code, settings)
        self.assertIn('CAST(name AS VARCHAR(500))', res)
        self.assertIn('WHEN GROUPING(col) = 1 THEN', res)

if __name__ == '__main__':
    unittest.main()
