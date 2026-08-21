# SPDX-License-Identifier: GPL-3.0-or-later
"""
The parameters of an anonymization method, as they arrive from YAML.

Every parameter is written in the configuration file, and a form-driven editor writes each
of them as text. A method which reads one as a plain truthy value therefore does the
opposite of what the configuration says as soon as it is written as "false" - the string,
which is truthy - and a masked copy that is not masked looks exactly like one that is.

Nothing here needs a database.

Run with:  python3 -m pytest tests/test_anonymization_method_parameters.py -v
"""

import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator.anonymization.methods import (
    deterministic_hash_mask, flag, partial_mask, postgres_anon_native)


# --------------------------------------------------------------------------------------
# pass_original of postgres_anon_native


@pytest.mark.parametrize('written', [False, 'false', 'False', 'no', 'off', '0', '', None])
def test_pass_original_written_as_a_no_does_not_pass_the_value(written):
    """The original value must not reach the function call unless it was asked for -
    'false' as a string used to do exactly that."""
    sql = postgres_anon_native('lisa@example.org',
                               {'func_name': 'anon.fake_email', 'pass_original': written})
    assert '%s' not in sql
    assert sql == '__RAW_SQL__:anon.fake_email()'


@pytest.mark.parametrize('written', [True, 'true', 'True', 'yes', 'on', '1'])
def test_pass_original_written_as_a_yes_passes_the_value(written):
    sql = postgres_anon_native('lisa@example.org',
                               {'func_name': 'anon.fake_email', 'pass_original': written})
    assert sql == '__RAW_SQL__:anon.fake_email(%s)'


def test_pass_original_is_absent_by_default():
    assert postgres_anon_native('x', {'func_name': 'anon.fake_city'}) == \
        '__RAW_SQL__:anon.fake_city()'


def test_the_arguments_are_kept_in_both_cases():
    params = {'func_name': 'anon.partial', 'args': "2, '***', 2"}
    assert postgres_anon_native('x', params) == "__RAW_SQL__:anon.partial(2, '***', 2)"
    assert postgres_anon_native('x', {**params, 'pass_original': 'true'}) == \
        "__RAW_SQL__:anon.partial(%s, 2, '***', 2)"


@pytest.mark.parametrize('written, expected', [
    ('true', True), ('yes', True), ('on', True), ('1', True), ('  TRUE  ', True),
    ('false', False), ('no', False), ('off', False), ('0', False), ('', False),
    (True, True), (False, False), (1, True), (0, False), (None, False),
])
def test_flag_reads_what_yaml_would_have_read(written, expected):
    assert flag({'p': written}, 'p') is expected


def test_flag_falls_back_to_its_default():
    assert flag({}, 'p') is False
    assert flag({}, 'p', default=True) is True


# --------------------------------------------------------------------------------------
# out_type of deterministic_hash_mask


@pytest.mark.parametrize('written', ['int', 'integer', 'INTEGER', ' Int '])
def test_out_type_gives_a_number_however_it_is_spelled(written):
    """docs/configs/anonymization_workflow.yaml documents 'integer' and
    docs/workflow/anonymization.md documents 'int'; both have to work, or the column asked
    for a number and receives a hex digest."""
    assert isinstance(deterministic_hash_mask('4711', {'out_type': written}), int)


@pytest.mark.parametrize('written', ['string', 'STRING', None])
def test_out_type_gives_the_digest_otherwise(written):
    params = {} if written is None else {'out_type': written}
    result = deterministic_hash_mask('4711', params)
    assert isinstance(result, str) and len(result) == 64


def test_the_hash_is_deterministic_and_salted():
    assert deterministic_hash_mask('a', {}) == deterministic_hash_mask('a', {})
    assert deterministic_hash_mask('a', {'salt': 'x'}) != deterministic_hash_mask('a', {})


# --------------------------------------------------------------------------------------
# the lengths of partial_mask, which arrive as text just as readily


def test_partial_mask_takes_its_lengths_as_text():
    assert partial_mask('4711123456', {'prefix_len': '4', 'suffix_len': '2',
                                       'mask_str': '*'}) == '4711*56'
