# SPDX-License-Identifier: GPL-3.0-or-later
"""
Behaviour of include_tables / exclude_tables and the same pairs for views and
functions/procedures, and of the pattern_syntax which drives them.

The point of these tests is that the six options behave IDENTICALLY. Every semantic
assertion is therefore run against all three object kinds.

Run with:  python3 -m pytest tests/test_object_filters.py -v
"""

import logging
import os
import sys
import types

import pytest
import yaml

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.config_parser import ConfigParser

OBJECT_KINDS = sorted(ConfigParser.OBJECT_FILTER_KEYS)

MINIMAL_CONFIG = {
    'migrator': {'type': 'postgresql', 'host': 'h', 'port': 5432, 'username': 'u',
                 'password': 'p', 'database': 'd', 'schema': 'migration'},
    'source':   {'type': 'oracle', 'host': 'h', 'port': 1521, 'username': 'u',
                 'password': 'p', 'database': 'd', 'schema': 's'},
    'target':   {'type': 'postgresql', 'host': 'h', 'port': 5432, 'username': 'u',
                 'password': 'p', 'database': 'd', 'schema': 'public'},
    'migration': {'names_case_handling': 'lower'},
}


def make_parser(tmp_path, **overrides):
    config = dict(MINIMAL_CONFIG)
    config.update(overrides)
    path = tmp_path / 'config.yaml'
    path.write_text(yaml.safe_dump(config), encoding='utf-8')
    logging.disable(logging.CRITICAL)
    try:
        return ConfigParser(
            types.SimpleNamespace(config=str(path), log_file=None, log_level='INFO'),
            logging.getLogger('test_object_filters'))
    finally:
        logging.disable(logging.NOTSET)


def selected(parser, kind, name):
    return parser.is_object_selected(kind, name)[0]


# --------------------------------------------------------------------------------------
# the same semantics for every object kind


@pytest.mark.parametrize('kind', OBJECT_KINDS)
@pytest.mark.parametrize('spelling', ['absent', 'all', 'empty_list', 'null', 'match_all_pattern'])
def test_include_meaning_everything_is_the_same_everywhere(tmp_path, kind, spelling):
    """
    Absent, 'all', [], null and a match-everything pattern must all select every object.
    An empty include list used to skip every view and every routine, silently.
    """
    include_option = ConfigParser.OBJECT_FILTER_KEYS[kind][0]
    values = {'all': 'all', 'empty_list': [], 'null': None, 'match_all_pattern': ['.*']}
    overrides = {} if spelling == 'absent' else {include_option: values[spelling]}
    parser = make_parser(tmp_path, **overrides)
    assert selected(parser, kind, 'ANY_OBJECT_NAME') is True


@pytest.mark.parametrize('kind', OBJECT_KINDS)
def test_absent_exclude_removes_nothing(tmp_path, kind):
    parser = make_parser(tmp_path)
    assert selected(parser, kind, 'anything') is True


@pytest.mark.parametrize('kind', OBJECT_KINDS)
def test_exclude_wins_over_include(tmp_path, kind):
    include_option, exclude_option = ConfigParser.OBJECT_FILTER_KEYS[kind]
    parser = make_parser(tmp_path, **{include_option: ['orders'], exclude_option: ['orders']})
    assert selected(parser, kind, 'orders') is False


@pytest.mark.parametrize('kind', OBJECT_KINDS)
def test_exclude_everything_really_excludes_everything(tmp_path, kind):
    """`exclude_*: ['.*']` used to exclude nothing, because '.*' was read as a glob."""
    exclude_option = ConfigParser.OBJECT_FILTER_KEYS[kind][1]
    parser = make_parser(tmp_path, **{exclude_option: ['.*']})
    assert selected(parser, kind, 'anything') is False


@pytest.mark.parametrize('kind', OBJECT_KINDS)
def test_matching_ignores_case(tmp_path, kind):
    include_option = ConfigParser.OBJECT_FILTER_KEYS[kind][0]
    parser = make_parser(tmp_path, **{include_option: ['orders']})
    assert selected(parser, kind, 'ORDERS') is True
    assert selected(parser, kind, 'Orders') is True


@pytest.mark.parametrize('kind', OBJECT_KINDS)
def test_pattern_must_match_the_whole_name(tmp_path, kind):
    """'orders' selects orders, not back_orders or orders_2024."""
    include_option = ConfigParser.OBJECT_FILTER_KEYS[kind][0]
    parser = make_parser(tmp_path, **{include_option: ['orders']})
    assert selected(parser, kind, 'orders') is True
    assert selected(parser, kind, 'back_orders') is False
    assert selected(parser, kind, 'orders_2024') is False


@pytest.mark.parametrize('kind', OBJECT_KINDS)
def test_a_bare_string_other_than_all_is_refused(tmp_path, kind):
    """
    A forgotten '- ' in the YAML must not quietly become a one-element list: the run would
    migrate a different set of objects than was meant. 'all' stays the one legal scalar.
    """
    exclude_option = ConfigParser.OBJECT_FILTER_KEYS[kind][1]
    with pytest.raises(ValueError) as caught:
        make_parser(tmp_path, **{exclude_option: 'tmp_data'})
    assert exclude_option in str(caught.value)

    parser = make_parser(tmp_path, **{exclude_option: 'all'})
    assert selected(parser, kind, 'anything') is False


# --------------------------------------------------------------------------------------
# pattern_syntax


def test_glob_is_the_default(tmp_path):
    assert make_parser(tmp_path).get_pattern_syntax() == 'glob'


@pytest.mark.parametrize('written,expected', [
    ('glob', 'glob'), ('GLOB', 'glob'), ('wildcard', 'glob'), ('fnmatch', 'glob'),
    ('regex', 'regex'), ('regexp', 'regex'), ('re', 'regex'),
    ('like', 'like'), ('sql_like', 'like'),
])
def test_pattern_syntax_aliases(tmp_path, written, expected):
    assert make_parser(tmp_path, pattern_syntax=written).get_pattern_syntax() == expected


def test_glob_semantics(tmp_path):
    parser = make_parser(tmp_path, pattern_syntax='glob', exclude_tables=['SYS*', 'TMP_?'])
    assert selected(parser, 'table', 'SYSTABLES') is False
    assert selected(parser, 'table', 'TMP_A') is False
    assert selected(parser, 'table', 'TMP_AB') is True     # ? is exactly one character
    assert selected(parser, 'table', 'orders') is True


def test_regex_semantics(tmp_path):
    parser = make_parser(tmp_path, pattern_syntax='regex', exclude_tables=[r'BIN\$.*'])
    assert selected(parser, 'table', 'BIN$X1') is False    # the example that never worked
    assert selected(parser, 'table', 'orders') is True


def test_like_semantics(tmp_path):
    parser = make_parser(tmp_path, pattern_syntax='like', exclude_tables=['TMP@_%'.replace('@', '\\')])
    assert selected(parser, 'table', 'TMP_ANYTHING') is False
    assert selected(parser, 'table', 'orders') is True

    single = make_parser(tmp_path, pattern_syntax='like', include_tables=['ORDER_'])
    assert selected(single, 'table', 'ORDERS') is True     # _ is exactly one character
    assert selected(single, 'table', 'ORDER') is False


def test_the_same_pattern_means_different_things_in_different_syntaxes(tmp_path):
    """This is the whole reason the setting exists - it must be honoured, not ignored."""
    as_glob = make_parser(tmp_path, pattern_syntax='glob', exclude_tables=['log_.*'])
    as_regex = make_parser(tmp_path, pattern_syntax='regex', exclude_tables=['log_.*'])
    assert selected(as_glob, 'table', 'log_2024') is True    # glob: needs a literal dot
    assert selected(as_regex, 'table', 'log_2024') is False


# --------------------------------------------------------------------------------------
# reporting and refusing


def test_an_unusable_pattern_stops_the_run(tmp_path):
    """A filter that silently matched nothing would migrate less than was asked for."""
    with pytest.raises(ValueError) as caught:
        make_parser(tmp_path, pattern_syntax='regex', exclude_tables=['[unclosed'])
    assert 'exclude_tables' in str(caught.value)
    assert 'pattern_syntax' in str(caught.value)


def test_an_unknown_pattern_syntax_stops_the_run(tmp_path):
    with pytest.raises(ValueError) as caught:
        make_parser(tmp_path, pattern_syntax='perl')
    assert 'pattern_syntax' in str(caught.value)


def test_a_scalar_other_than_all_stops_the_run(tmp_path):
    with pytest.raises(ValueError) as caught:
        make_parser(tmp_path, include_tables='orders')
    assert 'include_tables' in str(caught.value)


@pytest.mark.parametrize('syntax,pattern,expected_word', [
    ('glob', r'BIN\$.*', 'regular expression'),
    ('glob', 'log_.*', 'regular expression'),
    ('regex', 'SYS*', 'glob'),
    ('regex', 'TMP_%', 'LIKE'),
    ('like', 'SYS*', 'LIKE'),
])
def test_a_pattern_in_the_wrong_syntax_is_reported(syntax, pattern, expected_word):
    advice = ConfigParser.pattern_syntax_advice(pattern, syntax)
    assert advice is not None, f'{pattern!r} under {syntax} should have been reported'
    assert expected_word in advice


@pytest.mark.parametrize('syntax,pattern', [
    ('glob', 'SYS*'), ('glob', 'orders'), ('glob', 'TMP_?'),
    ('regex', r'BIN\$.*'), ('regex', 'orders'), ('regex', 'log_.*'),
    ('like', 'TMP%'), ('like', 'orders'), ('like', 'ORDER_'),
    ('glob', 'all'), ('regex', '.*'),
])
def test_a_pattern_in_the_right_syntax_is_not_reported(syntax, pattern):
    assert ConfigParser.pattern_syntax_advice(pattern, syntax) is None


def test_skipped_objects_are_counted_and_logged(tmp_path):
    parser = make_parser(tmp_path, exclude_tables=['tmp_*'])
    logging.disable(logging.CRITICAL)
    try:
        for name in ['orders', 'tmp_a', 'tmp_b', 'customers']:
            parser.report_object_selection('table', name, 'test')
    finally:
        logging.disable(logging.NOTSET)
    assert parser.object_filter_counters['table'] == {'selected': 2, 'skipped': 2}
