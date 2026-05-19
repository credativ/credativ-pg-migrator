import re
from credativ_pg_migrator.anonymization.registry import anonymization_registry
import credativ_pg_migrator.anonymization.methods  # Ensure methods are registered

class MigratorAnonymizer:
    def __init__(self, config):
        self.config = config
        self.anonymization_config = config.get('anonymization', {})
        self.tables_config = self.anonymization_config.get('tables', {})
        self.regex_config = self.anonymization_config.get('regex_mappings', {})
        
        # Precompile regexes for performance
        self.compiled_regexes = []
        for mapping in self.regex_config:
            table_pattern = mapping.get('table_pattern', '.*')
            column_pattern = mapping.get('column_pattern', '.*')
            self.compiled_regexes.append({
                'table_re': re.compile(table_pattern),
                'column_re': re.compile(column_pattern),
                'method': mapping.get('method'),
                'params': mapping.get('params', {})
            })

    def is_active(self):
        return bool(self.tables_config) or bool(self.compiled_regexes)

    def get_method_for_column(self, table_name, column_name):
        # 1. Check explicit table config first
        if table_name in self.tables_config:
            table_rules = self.tables_config[table_name]
            if column_name in table_rules:
                rule = table_rules[column_name]
                return rule.get('method'), rule.get('params', {})

        # 2. Check regex mappings
        for mapping in self.compiled_regexes:
            if mapping['table_re'].match(table_name) and mapping['column_re'].match(column_name):
                return mapping['method'], mapping['params']

        return None, {}

    def anonymize_row(self, table_name, row_dict):
        for col_name, value in row_dict.items():
            method_name, params = self.get_method_for_column(table_name, col_name)
            if method_name:
                func = anonymization_registry.get(method_name)
                if func:
                    row_dict[col_name] = func(value, params)
        return row_dict
