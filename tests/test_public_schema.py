import unittest
import yaml
import tempfile
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from credativ_pg_migrator.config_parser import ConfigParser

class ArgsMock:
    def __init__(self, config_path):
        self.config = config_path
        self.dry_run = False
        self.resume = False
        self.log_level = 'INFO'
        self.log_file = 'test.log'
        self.validate = False

class LoggerMock:
    def info(self, msg): pass
    def debug(self, msg): pass
    def warning(self, msg): pass
    def error(self, msg): pass

class TestPublicSchema(unittest.TestCase):
    def setUp(self):
        self.base_config = {
            'source': {
                'type': 'postgresql',
                'connectivity': 'native',
            },
            'target': {
                'type': 'postgresql',
            },
            'migration': {
                'names_case_handling': 'keep',
            },
            'include_tables': 'all',
        }

    def test_standard_workflow_public_schema_fails(self):
        config_data = self.base_config.copy()
        config_data['migration']['workflow'] = 'standard'
        config_data['migrator'] = {'type': 'postgresql', 'schema': 'public'}
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            f_path = f.name
            
        try:
            args = ArgsMock(f_path)
            with self.assertRaises(ValueError) as context:
                ConfigParser(args, LoggerMock())
            self.assertIn("Migrator protocol schema cannot be 'public'", str(context.exception))
        finally:
            os.remove(f_path)

    def test_mapping_workflow_public_schema_fails(self):
        config_data = self.base_config.copy()
        config_data['migration']['workflow'] = 'mapping'
        config_data['migrator'] = {'type': 'postgresql', 'schema': '  PUBLIC  '}
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            f_path = f.name
            
        try:
            args = ArgsMock(f_path)
            with self.assertRaises(ValueError) as context:
                ConfigParser(args, LoggerMock())
            self.assertIn("Migrator protocol schema cannot be 'public'", str(context.exception))
        finally:
            os.remove(f_path)

    def test_valid_schema_passes(self):
        config_data = self.base_config.copy()
        config_data['migration']['workflow'] = 'standard'
        config_data['migrator'] = {'type': 'postgresql', 'schema': 'my_migrator_schema'}
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            f_path = f.name
            
        try:
            args = ArgsMock(f_path)
            parser = ConfigParser(args, LoggerMock())
            self.assertEqual(parser.get_migrator_schema(), 'my_migrator_schema')
        finally:
            os.remove(f_path)

if __name__ == '__main__':
    unittest.main()
