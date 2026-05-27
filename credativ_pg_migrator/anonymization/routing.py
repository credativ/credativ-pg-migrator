import re
from credativ_pg_migrator.anonymization.registry import anonymization_registry
import credativ_pg_migrator.anonymization.methods  # Ensure methods are registered

def _compile_regex_robust(pattern):
    """
    Helper to compile regular expressions in a way that is robust to Python 3.11+
    strictness about global flags like (?i) not being at the start of the string.
    """
    flags_pattern = re.compile(r'\(\?([aiLmsxu]+)\)')
    flags_found = ''.join(flags_pattern.findall(pattern))
    clean_pattern = flags_pattern.sub('', pattern)
    
    if flags_found:
        flags_found = "".join(set(flags_found))
        clean_pattern = f"(?{flags_found}){clean_pattern}"
        
    return re.compile(clean_pattern)

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
                'table_re': _compile_regex_robust(table_pattern),
                'column_re': _compile_regex_robust(column_pattern),
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
