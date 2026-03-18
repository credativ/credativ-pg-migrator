import re
from typing import List, Dict, Tuple, Optional, Set
from collections import defaultdict

def normalize_name(name: str, rules: List[str] = None, settings: dict = None) -> str:
    """
    Normalizes a name based on a list of specified normalization methods.
    Available rules:
    - 'lowercase': Converts to lowercase
    - 'uppercase': Converts to uppercase
    - 'strip_trailing_numbers': Removes trailing numeric suffixes (e.g., '_123', '123')
    - 'strip_leading_numbers': Removes leading numeric prefixes (e.g., '123_', '123')
    - 'remove_underscores': Removes all underscores
    - 'alphanumeric_only': Removes all characters except letters and numbers
    - 'strip_prefixes': Removes prefixes specified in settings['prefixes']
    - 'strip_suffixes': Removes suffixes specified in settings['suffixes']
    - 'remove_vowels': Removes all vowels (a, e, i, o, u)
    """
    if not name: return ""

    if rules is None:
        rules = ['lowercase']

    result = name
    for rule in rules:
        if rule == 'lowercase':
            result = result.lower()
        elif rule == 'uppercase':
            result = result.upper()
        elif rule == 'strip_trailing_numbers':
            result = re.sub(r'_\d+$|\d+$', '', result)
        elif rule == 'strip_leading_numbers':
            result = re.sub(r'^_\d+|^\d+', '', result)
        elif rule == 'remove_underscores':
            result = result.replace('_', '')
        elif rule == 'alphanumeric_only':
            result = re.sub(r'[^a-zA-Z0-9]', '', result)
        elif rule == 'remove_vowels':
            result = re.sub(r'[aeiouAEIOU]', '', result)
        elif rule == 'strip_prefixes' and settings and 'prefixes' in settings:
            for p in settings['prefixes']:
                if result.startswith(p):
                    result = result[len(p):]
        elif rule == 'strip_suffixes' and settings and 'suffixes' in settings:
            for s in settings['suffixes']:
                if result.endswith(s):
                    result = result[:-len(s)]
    return result

def calculate_jaccard_similarity(source_cols: List[Dict], target_cols: List[Dict], rules: List[str] = None, settings: dict = None) -> float:
    names1 = set(normalize_name(c.get('name', ''), rules, settings) for c in source_cols)
    names2 = set(normalize_name(c.get('name', ''), rules, settings) for c in target_cols)

    if not names1 and not names2: return 1.0
    if not names1 or not names2: return 0.0

    intersection = names1.intersection(names2)
    union = names1.union(names2)
    return len(intersection) / len(union)

def calculate_enhanced_jaccard(source_cols: List[Dict], target_cols: List[Dict], prefixes: List[str], rules: List[str] = None, settings: dict = None) -> float:
    """Calculates Jaccard similarity with enhanced prefix/suffix handling."""
    score = calculate_jaccard_similarity(source_cols, target_cols, rules, settings)

    if score < 0.8:
        names1 = set(normalize_name(c.get('name', ''), rules, settings) for c in source_cols)
        names2 = set()
        for c in target_cols:
            raw_name = c.get('name', '')
            n = normalize_name(raw_name, rules, settings)
            names2.add(n)
            for p in prefixes:
                if raw_name.lower().startswith(p.lower()):
                    names2.add(normalize_name(raw_name[len(p):], rules, settings))

        if names1 and names2:
            score = len(names1 & names2) / len(names1 | names2)

    return score

def match_tables(settings: dict) -> dict:
    source_tables = settings.get('source_tables', [])
    target_tables = settings.get('target_tables', [])
    source_internal = settings.get('source_internal', {})
    target_internal = settings.get('target_internal', {})
    source_columns_map = settings.get('source_columns_map', {})
    target_columns_map = settings.get('target_columns_map', {})
    column_prefixes = settings.get('column_prefixes', ["gov_", "log_"])
    table_norm_rules = settings.get('table_normalization_rules', ['lowercase'])
    column_norm_rules = settings.get('column_normalization_rules', ['lowercase'])
    norm_settings = settings.get('normalization_settings', {})

    matched_pairs = []
    matched_source_set = set()
    matched_target_set = set()

    # 1. Internal Mapping Analysis
    internal_table_map = defaultdict(lambda: defaultdict(list))
    for prop, source_loc in source_internal.items():
        if prop in target_internal:
            target_loc = target_internal[prop]
            source_t, source_c = source_loc.split('.')
            target_t, target_c = target_loc.split('.')
            internal_table_map[source_t][target_t].append(prop)

    # Preserve correct casing across all matching phases
    true_case_source = {t.lower(): t for t in source_tables}
    true_case_target = {t.lower(): t for t in target_tables}

    def add_match(source_t, target_t, score, method, details, evidence):
        orig_source_t = source_t
        orig_target_t = target_t
        
        # Override with exact database casing for robust downstream map lookups
        source_t = true_case_source.get(source_t.lower(), source_t)
        target_t = true_case_target.get(target_t.lower(), target_t)

        stats = {}
        stats['exact_name'] = (source_t.lower() == target_t.lower())
        stats['normalized_name'] = (normalize_name(source_t, table_norm_rules, norm_settings) == normalize_name(target_t, table_norm_rules, norm_settings))
        votes = internal_table_map.get(orig_source_t, {}).get(orig_target_t, [])
        stats['internal_mapping'] = len(votes)

        source_cols = source_columns_map.get(source_t, [])
        target_cols = target_columns_map.get(target_t, [])
        stats['jaccard'] = calculate_enhanced_jaccard(source_cols, target_cols, column_prefixes, column_norm_rules, norm_settings)

        matched_pairs.append({
            'source_table': source_t,
            'target_table': target_t,
            'score': score,
            'method': method,
            'details': details,
            'evidence': evidence,
            'stats': stats
        })
        matched_source_set.add(source_t.lower())
        matched_target_set.add(target_t.lower())

    # Phase 1: Internal Mapping Match
    for source_t, target_votes in internal_table_map.items():
        best_target = max(target_votes, key=lambda k: len(target_votes[k]))
        props = target_votes[best_target]
        score = len(props)
        if source_t.lower() not in matched_source_set and best_target.lower() not in matched_target_set:
             add_match(source_t, best_target, 100, "Internal Mapping", f"Matched via {score} properties", props)

    # Phase 2: Name Matching
    target_norm_map = {}
    for t in target_tables:
        if t.lower() not in matched_target_set:
            norm = normalize_name(t.lower(), table_norm_rules, norm_settings)
            target_norm_map[norm] = t.lower()

    for t_source_raw in source_tables:
        t_source = t_source_raw.lower()
        if t_source in matched_source_set: continue

        norm_source = normalize_name(t_source, table_norm_rules, norm_settings)

        if t_source in [t.lower() for t in target_tables] and t_source not in matched_target_set:
            # find original case for target table
            t_target_orig = next(t for t in target_tables if t.lower() == t_source)
            add_match(t_source_raw, t_target_orig, 100, "Exact Name", "Same table name", None)
        elif norm_source in target_norm_map:
            t_target_lower = target_norm_map[norm_source]
            t_target_orig = next(t for t in target_tables if t.lower() == t_target_lower)
            add_match(t_source_raw, t_target_orig, 95, "Normalized Name", f"Normalized: {norm_source}", None)
            del target_norm_map[norm_source]

    # Phase 3: Advanced Column Matching (Jaccard)
    unmatched_source_list = [t for t in source_tables if t.lower() not in matched_source_set]
    unmatched_target_list = [t for t in target_tables if t.lower() not in matched_target_set]

    for t_source in unmatched_source_list:
        if t_source.lower() in matched_source_set: continue
        source_cols = source_columns_map.get(t_source, [])
        best_target = None
        best_jaccard = 0.0

        for t_target in unmatched_target_list:
            if t_target.lower() in matched_target_set: continue
            target_cols = target_columns_map.get(t_target, [])

            score = calculate_enhanced_jaccard(source_cols, target_cols, column_prefixes, column_norm_rules, norm_settings)

            if score > best_jaccard:
                best_jaccard = score
                best_target = t_target

        if best_jaccard > 0.5:
             add_match(t_source, best_target, int(best_jaccard * 100), "Column Fingerprint", f"Jaccard: {best_jaccard:.2f}", None)

    final_unmatched_source = [t for t in source_tables if t.lower() not in matched_source_set]
    final_unmatched_target = [t for t in target_tables if t.lower() not in matched_target_set]

    return {
        'matched_pairs': matched_pairs,
        'unmatched_source': final_unmatched_source,
        'unmatched_target': final_unmatched_target
    }

def match_columns(settings: dict) -> dict:
    source_columns = settings.get('source_columns', [])
    target_columns = settings.get('target_columns', [])
    column_prefixes = settings.get('column_prefixes', ['gov_', 'log_'])
    column_norm_rules = settings.get('column_normalization_rules', ['lowercase'])
    norm_settings = settings.get('normalization_settings', {})

    matched = []
    target_col_map = {c.get('name', '').lower(): c for c in target_columns}
    target_col_norm_map = {}
    for c in target_columns:
        c_name = c.get('name', '')
        target_col_norm_map[normalize_name(c_name, column_norm_rules, norm_settings)] = c
        for p in column_prefixes:
            if c_name.lower().startswith(p.lower()):
                 target_col_norm_map[normalize_name(c_name[len(p):], column_norm_rules, norm_settings)] = c

    matched_source_names = set()
    matched_target_names = set()

    for c_source in source_columns:
        c_source_name = c_source.get('name', '')
        if c_source_name.lower() in target_col_map:
             c_target = target_col_map[c_source_name.lower()]
             c_target_name = c_target.get('name', '')
             if c_target_name not in matched_target_names:
                 matched.append({
                     'source_column': c_source,
                     'target_column': c_target,
                     'method': "Exact"
                 })
                 matched_source_names.add(c_source_name)
                 matched_target_names.add(c_target_name)
                 continue

    for c_source in source_columns:
        c_source_name = c_source.get('name', '')
        if c_source_name in matched_source_names: continue
        norm = normalize_name(c_source_name, column_norm_rules, norm_settings)
        if norm in target_col_norm_map:
            c_target = target_col_norm_map[norm]
            c_target_name = c_target.get('name', '')
            if c_target_name not in matched_target_names:
                matched.append({
                     'source_column': c_source,
                     'target_column': c_target,
                     'method': "Fuzzy/Normalized"
                 })
                matched_source_names.add(c_source_name)
                matched_target_names.add(c_target_name)

    unmatched_source = [c for c in source_columns if c.get('name', '') not in matched_source_names]
    unmatched_target = [c for c in target_columns if c.get('name', '') not in matched_target_names]

    return {
        'matched_columns': matched,
        'unmatched_source': unmatched_source,
        'unmatched_target': unmatched_target
    }
