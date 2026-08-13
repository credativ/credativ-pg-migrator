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


class AnonymizationValueTooLongError(ValueError):
    """
    A value does not fit into the target column and the configuration does not say what to do
    with it. Cutting it silently destroys data and hides the real problem - a target column
    narrower than the source data, or a method producing longer output than the original.
    """
    pass


# anonymization.on_value_too_long - what to do with a string value which does not fit into the
# length of the target column
VALUE_TOO_LONG_POLICIES = (
    # stop the migration and report the column - the default
    'error',
    # cut the value to the length of the column, counted and reported, never silent
    'fit',
    # call the anonymization method again until its result fits, stop when it cannot
    'find_fitting_value',
)
DEFAULT_FIND_FITTING_VALUE_ATTEMPTS = 10
# a value produced for the target server ("__RAW_SQL__:anon.fake_city()") is a function call,
# not the data - its length says nothing about the length of the value the target will store
RAW_SQL_PREFIX = '__RAW_SQL__:'


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
        self._check_value_too_long_config(errors)
        if errors:
            raise AnonymizationConfigError(
                "Invalid 'anonymization' configuration - the run is stopped because the listed columns "
                "would keep their original values while the migration reports success:"
                + "".join(f"\n  - {error}" for error in errors))

    def _check_value_too_long_config(self, errors):
        self.on_value_too_long = self.anonymization_config.get('on_value_too_long', 'error')
        self.find_fitting_value_attempts = self.anonymization_config.get(
            'find_fitting_value_attempts', DEFAULT_FIND_FITTING_VALUE_ATTEMPTS)

        if self.on_value_too_long not in VALUE_TOO_LONG_POLICIES:
            errors.append(f"anonymization.on_value_too_long: '{self.on_value_too_long}' is not a known policy - "
                          f"use one of: {', '.join(VALUE_TOO_LONG_POLICIES)}")
            self.on_value_too_long = 'error'

        try:
            self.find_fitting_value_attempts = int(self.find_fitting_value_attempts)
        except (TypeError, ValueError):
            errors.append(f"anonymization.find_fitting_value_attempts: '{self.find_fitting_value_attempts}' is not a number")
            self.find_fitting_value_attempts = DEFAULT_FIND_FITTING_VALUE_ATTEMPTS
        if self.find_fitting_value_attempts < 1:
            errors.append(f"anonymization.find_fitting_value_attempts: {self.find_fitting_value_attempts} - at least one attempt is needed")
            self.find_fitting_value_attempts = DEFAULT_FIND_FITTING_VALUE_ATTEMPTS

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

    def anonymize_row(self, table_name, row_dict, stats=None, max_lengths=None, length_stats=None):
        """
        Replace every value covered by a rule. NULL values are left untouched - they carry no
        personal data. If 'stats' is a dict, it is filled with
        { (column_name, method_name): number of values replaced }.

        When 'max_lengths' is given as { column_name: length of the target column }, every
        string value which does not fit is handled according to anonymization.on_value_too_long -
        for the columns carrying a rule and for the columns copied as they are. The counts of
        the values which had to be changed to fit end in 'length_stats'.
        """
        for col_name, value in row_dict.items():
            method_name = None
            params = {}
            func = None

            if value is not None:
                method_name, params = self.get_method_for_column(table_name, col_name)
                if method_name:
                    func = anonymization_registry.get(method_name)
                    if func is None:
                        # Startup validation should have caught this - but an unresolvable method
                        # must never end as a skipped column with the original value copied to
                        # the target.
                        raise AnonymizationConfigError(
                            f"anonymization: table '{table_name}', column '{col_name}': method '{method_name}' "
                            f"is not registered - known methods are: {', '.join(anonymization_registry.names())}")
                    anonymized_value = func(value, params)
                    row_dict[col_name] = anonymized_value
                    if stats is not None:
                        stats_key = (col_name, method_name)
                        stats[stats_key] = stats.get(stats_key, 0) + 1
                else:
                    anonymized_value = value

                if max_lengths:
                    max_length = max_lengths.get(col_name)
                    if (max_length and isinstance(anonymized_value, str)
                            and not anonymized_value.startswith(RAW_SQL_PREFIX)
                            and len(anonymized_value) > max_length):
                        row_dict[col_name] = self._fit_value({
                            'table_name': table_name,
                            'column_name': col_name,
                            'original_value': value,
                            'value': anonymized_value,
                            'max_length': max_length,
                            'method_name': method_name,
                            'params': params,
                            'func': func,
                            'length_stats': length_stats,
                        })
        return row_dict

    def _count_length_event(self, length_stats, column_name, event):
        if length_stats is None:
            return
        counts = length_stats.setdefault(column_name, {'truncated': 0, 'refitted': 0})
        counts[event] += 1

    def _fit_value(self, settings):
        """
        A string value which is longer than the target column. The value itself is never part of
        a message - it is the personal data this workflow exists to protect.
        """
        table_name = settings['table_name']
        column_name = settings['column_name']
        original_value = settings['original_value']
        value = settings['value']
        max_length = settings['max_length']
        method_name = settings['method_name']
        params = settings['params']
        func = settings['func']
        length_stats = settings['length_stats']

        rule_description = f"rule '{method_name}'" if method_name else "no anonymization rule"
        location = f"anonymization: table '{table_name}', column '{column_name}' ({rule_description})"

        if self.on_value_too_long == 'fit':
            self._count_length_event(length_stats, column_name, 'truncated')
            return value[:max_length]

        if self.on_value_too_long == 'find_fitting_value':
            if func is None:
                raise AnonymizationValueTooLongError(
                    f"{location}: a value of {len(value)} characters does not fit into the target column "
                    f"({max_length} characters) and the column has no anonymization rule, so no other value "
                    f"can be generated for it. Widen the target column, or set "
                    f"anonymization.on_value_too_long to 'fit' to cut such values.")

            candidate = value
            for attempt in range(1, self.find_fitting_value_attempts + 1):
                new_candidate = func(original_value, params)
                if not isinstance(new_candidate, str):
                    # the method stopped producing a string - the length of the target column
                    # does not describe such a value any more
                    return new_candidate
                if len(new_candidate) <= max_length:
                    self._count_length_event(length_stats, column_name, 'refitted')
                    return new_candidate
                if new_candidate == candidate:
                    raise AnonymizationValueTooLongError(
                        f"{location}: method '{method_name}' returns the same value of {len(new_candidate)} "
                        f"characters every time, it cannot produce one fitting into the target column "
                        f"({max_length} characters). Widen the target column, choose a method producing "
                        f"shorter values, or set anonymization.on_value_too_long to 'fit' to cut such values.")
                candidate = new_candidate

            raise AnonymizationValueTooLongError(
                f"{location}: method '{method_name}' did not produce a value fitting into the target column "
                f"({max_length} characters) in {self.find_fitting_value_attempts} attempts. Raise "
                f"anonymization.find_fitting_value_attempts, widen the target column, or set "
                f"anonymization.on_value_too_long to 'fit' to cut such values.")

        raise AnonymizationValueTooLongError(
            f"{location}: a value of {len(value)} characters does not fit into the target column "
            f"({max_length} characters). Cutting it would destroy data silently - set "
            f"anonymization.on_value_too_long to 'fit' to cut such values, or to 'find_fitting_value' "
            f"to let the anonymization method produce a value which fits.")
