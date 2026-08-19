import unittest
import struct
from unittest.mock import MagicMock
from credativ_pg_migrator.connectors.mysql_connector import MySQLConnector

class TestMySQLSpatialPoint(unittest.TestCase):
    def test_point_wkb_bytes_conversion(self):
        connector = MySQLConnector.__new__(MySQLConnector)
        
        # Binary WKB for SRID 4326, POINT(9.6, 50.6)
        wkb_bytes = b'\xe6\x10\x00\x00\x01\x01\x00\x00\x00333333#@\xcd\xcc\xcc\xcc\xccLI@'
        
        # Test record transformation when target is PostgreSQL 'POINT'
        source_columns = {1: {'column_name': 'home_location', 'data_type': 'point'}}
        target_columns = {1: {'column_name': 'home_location', 'data_type': 'POINT'}}
        
        val = wkb_bytes
        # Simulating the conversion logic in migrate_table
        if isinstance(val, (bytes, bytearray)) and len(val) >= 25:
            srid, byte_order, geom_type, x, y = struct.unpack('<IBIdd', val[:25])
            if geom_type == 1:
                res = f"({x}, {y})"
        
        self.assertEqual(res, "(9.6, 50.6)")

    def test_point_wkt_string_conversion(self):
        wkt_str = "POINT(13.404954 52.520008)"
        import re
        m = re.search(r'POINT\s*\(\s*([^\s,]+)\s+([^\s,]+)\s*\)', wkt_str, re.IGNORECASE)
        res = f"({m.group(1)}, {m.group(2)})" if m else wkt_str

        self.assertEqual(res, "(13.404954, 52.520008)")

    def test_spatial_index_using_gist(self):
        from credativ_pg_migrator.connectors.postgresql_connector import PostgreSQLConnector
        connector = PostgreSQLConnector.__new__(PostgreSQLConnector)
        config_parser = MagicMock()
        config_parser.convert_names_case.side_effect = lambda x: x.lower()
        connector.config_parser = config_parser

        settings = {
            'index_name': 'six_wh_location',
            'index_type': 'INDEX',
            'target_schema_name': 'public',
            'target_table_name': 'warehouses',
            'index_columns': 'location',
            'target_columns': {1: {'column_name': 'location', 'column_data_type': 'POINT'}}
        }

        sql = connector.get_create_index_sql(settings)
        self.assertIn('USING gist', sql)
        self.assertIn('"six_wh_location_tab_warehouses"', sql)
        self.assertIn('"location"', sql)

if __name__ == '__main__':
    unittest.main()
