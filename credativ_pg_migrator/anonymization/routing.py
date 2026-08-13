import re
from credativ_pg_migrator.anonymization.registry import anonymization_registry
import credativ_pg_migrator.anonymization.methods  # Ensure methods are registered


class AnonymizationConfigError(ValueError):
    """
    A fatal problem in the anonymization section of the config file.

    An unusable rule must never be skipped at runtime - the column would keep its original
    personal data while the run reports success.
    """
    pass


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
        self.anonymization_config = config.get('anonymization') or {}
        self.tables_config = self.anonymization_config.get('tables') or {}
        self.regex_config = self.anonymization_config.get('regex_mappings') or []

        # Every rule is checked here, at construction time, so that an unknown method or a
        # broken pattern stops the run before any data is read - never during the copy, where
        # the only visible effect would be personal data landing unchanged in the target.
        errors = []
        self.rules_count = 0
        self.compiled_regexes = []
        self._check_tables_config(errors)
        self._compile_regex_mappings(errors)
        if errors:
            raise AnonymizationConfigError(
                "Invalid 'anonymization' configuration - the run is stopped because the listed columns "
                "would keep their original values while the migration reports success:"
                + "".join(f"\n  - {error}" for error in errors))

    def _check_method(self, method_name, location, errors):
        if method_name is None or method_name == '':
            errors.append(f"{location}: 'method' is missing or empty")
            return
        if not isinstance(method_name, str):
            errors.append(f"{location}: 'method' must be a string, found {type(method_name).__name__}")
            return
        if not anonymization_registry.is_registered(method_name):
            errors.append(f"{location}: unknown anonymization method '{method_name}' - "
                          f"known methods are: {', '.join(anonymization_registry.names())}")

    def _check_params(self, params, location, errors):
        if params is not None and not isinstance(params, dict):
            errors.append(f"{location}: 'params' must be a mapping, found {type(params).__name__}")

    def _check_tables_config(self, errors):
        if not isinstance(self.tables_config, dict):
            errors.append("anonymization.tables must be a mapping of table name -> column rules, "
                          f"found {type(self.tables_config).__name__}")
            self.tables_config = {}
            return

        for table_name, table_rules in self.tables_config.items():
            if not isinstance(table_rules, dict):
                errors.append(f"anonymization.tables.{table_name} must be a mapping of column name -> rule, "
                              f"found {type(table_rules).__name__}")
                continue
            for column_name, rule in table_rules.items():
                location = f"anonymization.tables.{table_name}.{column_name}"
                if not isinstance(rule, dict):
                    errors.append(f"{location} must be a mapping containing at least 'method', "
                                  f"found {type(rule).__name__}")
                    continue
                self.rules_count += 1
                self._check_method(rule.get('method'), location, errors)
                self._check_params(rule.get('params'), location, errors)

    def _compile_regex_mappings(self, errors):
        if not isinstance(self.regex_config, list):
            errors.append("anonymization.regex_mappings must be a list of mappings, "
                          f"found {type(self.regex_config).__name__}")
            self.regex_config = []
            return

        for position, mapping in enumerate(self.regex_config, start=1):
            location = f"anonymization.regex_mappings[{position}]"
            if not isinstance(mapping, dict):
                errors.append(f"{location} must be a mapping with 'table_pattern', 'column_pattern' and 'method', "
                              f"found {type(mapping).__name__}")
                continue

            self.rules_count += 1
            table_pattern = mapping.get('table_pattern', '.*')
            column_pattern = mapping.get('column_pattern', '.*')

            table_re = None
            column_re = None
            try:
                table_re = _compile_regex_robust(table_pattern)
            except (re.error, TypeError) as e:
                errors.append(f"{location}: invalid 'table_pattern' {table_pattern!r} - {e}")
            try:
                column_re = _compile_regex_robust(column_pattern)
            except (re.error, TypeError) as e:
                errors.append(f"{location}: invalid 'column_pattern' {column_pattern!r} - {e}")

            self._check_method(mapping.get('method'), location, errors)
            self._check_params(mapping.get('params'), location, errors)

            if table_re is not None and column_re is not None:
                self.compiled_regexes.append({
                    'table_re': table_re,
                    'column_re': column_re,
                    'method': mapping.get('method'),
                    'params': mapping.get('params') or {}
                })

    def is_active(self):
        return bool(self.tables_config) or bool(self.compiled_regexes)

    def get_method_for_column(self, table_name, column_name):
        # 1. Check explicit table config first
        if table_name in self.tables_config:
            table_rules = self.tables_config[table_name]
            if column_name in table_rules:
                rule = table_rules[column_name]
                return rule.get('method'), (rule.get('params') or {})

        # 2. Check regex mappings
        for mapping in self.compiled_regexes:
            if mapping['table_re'].match(table_name) and mapping['column_re'].match(column_name):
                return mapping['method'], mapping['params']

        return None, {}

    def get_rules_for_columns(self, table_name, column_names):
        """
        Rules that apply to the given columns of the given table, as
        { column_name: (method_name, params) }. Used to record which rules really fired.
        """
        rules = {}
        for column_name in column_names:
            method_name, params = self.get_method_for_column(table_name, column_name)
            if method_name:
                rules[column_name] = (method_name, params)
        return rules

    def anonymize_row(self, table_name, row_dict, stats=None):
        """
        Replace every value covered by a rule. NULL values are left untouched - they carry no
        personal data. If 'stats' is a dict, it is filled with
        { (column_name, method_name): number of values replaced }.
        """
        for col_name, value in row_dict.items():
            if value is None:
                continue
            method_name, params = self.get_method_for_column(table_name, col_name)
            if not method_name:
                continue
            func = anonymization_registry.get(method_name)
            if func is None:
                # Startup validation should have caught this - but an unresolvable method must
                # never end as a skipped column with the original value copied to the target.
                raise AnonymizationConfigError(
                    f"anonymization: table '{table_name}', column '{col_name}': method '{method_name}' "
                    f"is not registered - known methods are: {', '.join(anonymization_registry.names())}")
            row_dict[col_name] = func(value, params)
            if stats is not None:
                stats_key = (col_name, method_name)
                stats[stats_key] = stats.get(stats_key, 0) + 1
        return row_dict
