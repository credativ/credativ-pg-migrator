# credativ-pg-migrator
# Copyright (C) 2025 credativ GmbH
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""
The fragmentation of Informix: what `sysfragments` says, and the small part of it PostgreSQL can
be given.

§2.4 of development/PARTITIONING_STRATEGY.md gives informix a verdict none of the other eleven
sources has - *"the case where the honest report is **none of this should be reproduced**, which
is worth more than a translation"* - and this file exists to hold the connector to both halves of
that. Everything `sysfragments` holds is **reported**, including the skew and the dbspaces and
the strategies which are not a partitioning at all; and a scheme is **built** only where the
fragments really are a range or a list over one column, which is the minority case.

The one thing which makes an Informix expression scheme readable is first-match: Informix
evaluates the fragments in `evalpos` order and stops at the first true one, so a chain of
`col < v` is a chain of ranges and not a set of overlapping ones. Read literally, copying that
pair into PostgreSQL bounds would be refused by the target as overlapping partitions.

Nothing here needs a database or an Informix client.
"""

import os
import sys
import pytest
from unittest.mock import MagicMock

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from credativ_pg_migrator import partitioning
from credativ_pg_migrator.connectors import informix_partitioning as ifx
from credativ_pg_migrator.connectors.informix_partitioning import UntranslatableScheme
from credativ_pg_migrator.connectors.informix_connector import InformixConnector


def fragment(name, expression, rows=None, dbspace='dbs1', evalpos=0):
    return ifx.Fragment(name=name, dbspace=dbspace, expression=expression, rows=rows,
                        evalpos=evalpos, is_remainder=not expression)


def scheme_of(strategy, fragments, table='ORDERS'):
    method, columns, notes, blockers = ifx.build_scheme(table, strategy, fragments, str.lower)
    return {'method': method, 'columns': columns, 'notes': notes, 'blockers': blockers,
            'bounds': [f.target_bound for f in fragments]}


# --------------------------------------------------------------------------------------
# one value out of a fragment expression


@pytest.mark.parametrize('written, expected', [
    ('100', '100'),
    ('-1.5', '-1.5'),
    ("'DE'", "'DE'"),
    ("DATE('2024-01-01')", "'2024-01-01'"),
    ('MDY(1,1,2024)', "'2024-01-01'"),
    ('MDY(12,31,2023)', "'2023-12-31'"),
    ('DATETIME(2024-01-01 00:00:00) YEAR TO SECOND', "'2024-01-01 00:00:00'"),
])
def test_a_bound_value_is_written_the_way_postgresql_writes_it(written, expected):
    assert ifx.to_postgresql_value(written) == expected


@pytest.mark.parametrize('written', [
    ## a bound which has to be evaluated is a boundary which moves - the partition would not be
    ## the partition the source had, and nothing later in the run would notice
    'TODAY',
    'CURRENT YEAR TO SECOND',
    'other_column',
    'YEAR(order_date)',
    '',
])
def test_a_bound_which_is_not_a_literal_is_refused(written):
    with pytest.raises(UntranslatableScheme):
        ifx.to_postgresql_value(written)


# --------------------------------------------------------------------------------------
# the brackets and the ANDs, which is where a parser written by hand goes wrong


@pytest.mark.parametrize('written, expected', [
    ('(a < 1)', 'a < 1'),
    ('((a < 1))', 'a < 1'),
    ## these brackets do NOT wrap the whole expression and both have to stay
    ('(a < 1) AND (b > 2)', '(a < 1) AND (b > 2)'),
    ("DATE('2024-01-01')", "DATE('2024-01-01')"),
    ('a < 1', 'a < 1'),
])
def test_only_the_brackets_which_wrap_the_whole_expression_are_taken_off(written, expected):
    assert ifx.strip_outer_brackets(written) == expected


def test_a_bracketed_function_call_keeps_its_own_bracket():
    """
    Stripping with an optional bracket at each end of the comparison instead turns
    `DATE('2024-01-01')` into `DATE('2024-01-01'` - a value which cannot be read, out of a bound
    which was perfectly good, and the whole scheme is then refused for nothing.
    """
    read = ifx.read_fragment(fragment('p1', "(order_date < DATE('2024-01-01'))"))
    assert read.high == "'2024-01-01'"


@pytest.mark.parametrize('written, parts', [
    ('a < 1 AND b > 2', ['a < 1', 'b > 2']),
    ## the AND inside a literal is not a separator, and splitting on it reads two comparisons
    ## which are not there
    ("region = 'X AND Y'", ["region = 'X AND Y'"]),
    ('(a < 1) AND (b > 2)', ['(a < 1)', '(b > 2)']),
    ('a IN (1, 2)', ['a IN (1, 2)']),
])
def test_an_expression_is_split_on_the_ands_which_are_separators(written, parts):
    assert ifx.split_top_level_and(written) == parts


# --------------------------------------------------------------------------------------
# what a fragment turns out to be


def test_a_chain_of_upper_bounds_is_a_chain_of_ranges():
    """
    First-match: Informix stops at the first fragment whose expression is true, so the second
    fragment really holds 2023 - the first already took everything below it. Read literally the
    second expression covers the first, and PostgreSQL would refuse the two as overlapping.
    """
    fragments = [
        fragment('orders_p1', "order_date < DATE('2023-01-01')", 5000),
        fragment('orders_p2', "order_date < DATE('2024-01-01')", 6000),
        fragment('orders_p3', '', 100),
    ]
    scheme = scheme_of('E', fragments)
    assert scheme['method'] == 'RANGE' and scheme['columns'] == ['order_date']
    assert scheme['bounds'] == [
        "FOR VALUES FROM (MINVALUE) TO ('2023-01-01')",
        "FOR VALUES FROM ('2023-01-01') TO ('2024-01-01')",
        'DEFAULT',
    ]
    assert scheme['blockers'] == []


def test_a_fragment_which_writes_both_of_its_ends_is_taken_as_written():
    fragments = [
        fragment('p1', "(acct >= 0 AND acct < 100)"),
        fragment('p2', "(acct >= 100 AND acct < 200)"),
    ]
    scheme = scheme_of('E', fragments)
    assert scheme['bounds'] == ['FOR VALUES FROM (0) TO (100)',
                                'FOR VALUES FROM (100) TO (200)']


def test_the_last_fragment_of_a_chain_may_be_open_at_the_top():
    fragments = [fragment('p1', 'acct < 100'), fragment('p2', 'acct >= 100')]
    scheme = scheme_of('E', fragments)
    assert scheme['bounds'] == ['FOR VALUES FROM (MINVALUE) TO (100)',
                                'FOR VALUES FROM (100) TO (MAXVALUE)']


def test_a_list_of_values_becomes_a_list_partitioning():
    fragments = [
        fragment('cust_emea', "region IN ('DE','FR')"),
        fragment('cust_amer', "region IN ('US')"),
        fragment('cust_rest', ''),
    ]
    scheme = scheme_of('E', fragments, table='CUSTOMERS')
    assert scheme['method'] == 'LIST' and scheme['columns'] == ['region']
    assert scheme['bounds'] == ["FOR VALUES IN ('DE', 'FR')", "FOR VALUES IN ('US')", 'DEFAULT']


def test_an_equality_is_a_list_of_one():
    scheme = scheme_of('E', [fragment('p1', "region = 'DE'"), fragment('p2', "region = 'FR'")])
    assert scheme['method'] == 'LIST'
    assert scheme['bounds'] == ["FOR VALUES IN ('DE')", "FOR VALUES IN ('FR')"]


def test_the_remainder_becomes_the_default_partition_and_what_it_costs_is_said():
    fragments = [fragment('p1', 'acct < 100'), fragment('p_rest', '')]
    scheme = scheme_of('E', fragments)
    assert scheme['bounds'][-1] == 'DEFAULT'
    assert any('DEFAULT partition' in note and 'costs' in note for note in scheme['notes'])


# --------------------------------------------------------------------------------------
# what is refused, and why


def test_round_robin_has_no_key_at_all():
    """
    Informix puts each new row in the next dbspace in turn. There is nothing about a row which
    decides where it goes, and PostgreSQL routes a row by its value.
    """
    scheme = scheme_of('R', [fragment('p1', ''), fragment('p2', '')])
    assert scheme['method'] == ''
    assert len(scheme['blockers']) == 1
    assert 'no partitioning key' in scheme['blockers'][0]
    assert 'source_partitioning: flatten' in scheme['blockers'][0]


def test_a_hybrid_scheme_is_refused():
    scheme = scheme_of('H', [fragment('p1', 'acct < 100')])
    assert scheme['method'] == ''
    assert any('HYBRID' in issue for issue in scheme['blockers'])


def test_an_arbitrary_boolean_expression_is_a_rewrite_and_not_a_translation():
    """§4.2: PostgreSQL partitions by a range, a list or a hash, and this is none of the three."""
    scheme = scheme_of('E', [fragment('p1', "(status = 'A' AND balance > 100 AND region <> 'X')")])
    assert scheme['method'] == ''
    assert any('more than two comparisons' in issue for issue in scheme['blockers'])


def test_an_expression_which_names_two_columns_is_refused():
    scheme = scheme_of('E', [fragment('p1', 'acct >= 0 AND region < 5')])
    assert any('more than one column' in issue for issue in scheme['blockers'])


def test_fragments_which_test_different_columns_are_refused():
    scheme = scheme_of('E', [fragment('p1', 'acct < 100'), fragment('p2', "region = 'DE'")])
    assert scheme['method'] == ''
    assert scheme['blockers']


def test_two_fragments_which_overlap_are_refused_because_postgresql_refuses_them():
    """
    Informix takes the first fragment whose expression is true, so only its order says which row
    goes where. PostgreSQL has no such rule and refuses the second CREATE TABLE - which is a run
    that fails in the middle rather than one which was stopped before it began.
    """
    fragments = [fragment('p1', '(acct >= 0 AND acct < 200)'),
                 fragment('p2', '(acct >= 100 AND acct < 300)')]
    scheme = scheme_of('E', fragments)
    assert scheme['method'] == ''
    assert any('overlap' in issue for issue in scheme['blockers'])


def test_a_bound_this_migrator_cannot_order_is_refused_rather_than_assumed():
    """
    Whether two bounds overlap has to be decided, and where it cannot be decided the scheme is
    refused: PostgreSQL refuses overlapping partitions and finding that out on the second CREATE
    TABLE is what this exists to prevent.
    """
    fragments = [fragment('p1', "(acct >= 'a' AND acct < 100)"),
                 fragment('p2', "(acct >= 'b' AND acct < 200)")]
    scheme = scheme_of('E', fragments)
    assert any('cannot tell which of the two is the lower' in issue
               for issue in scheme['blockers'])


def test_an_inclusive_upper_bound_is_refused_rather_than_moved():
    """
    PostgreSQL's upper bound is always exclusive, and the next value of the type is not
    something a fragment expression says - it does not carry the type at all.
    """
    scheme = scheme_of('E', [fragment('p1', 'acct <= 99')])
    assert any('INCLUSIVE' in issue for issue in scheme['blockers'])


def test_a_lower_bound_which_excludes_its_own_value_is_refused():
    scheme = scheme_of('E', [fragment('p1', 'acct > 99')])
    assert any('AFTER a value' in issue for issue in scheme['blockers'])


def test_two_fragments_listing_the_same_value_are_refused():
    fragments = [fragment('p1', "region IN ('DE','FR')"), fragment('p2', "region IN ('FR')")]
    scheme = scheme_of('E', fragments)
    assert any("'FR'" in issue and 'both' in issue for issue in scheme['blockers'])


def test_two_remainders_are_more_than_postgresql_takes():
    fragments = [fragment('p1', 'acct < 100'), fragment('p2', ''), fragment('p3', '')]
    scheme = scheme_of('E', fragments)
    assert any('one DEFAULT partition' in issue for issue in scheme['blockers'])


def test_a_scheme_with_a_blocker_builds_nothing_at_all():
    """Half a partitioning is a table which refuses the rows of the half which is missing."""
    fragments = [fragment('p1', 'acct < 100'), fragment('p2', 'acct <= 200')]
    scheme = scheme_of('E', fragments)
    assert scheme['method'] == '' and scheme['columns'] == []


# --------------------------------------------------------------------------------------
# the report, which for this source is the point


def test_the_report_says_a_fragment_is_a_place_and_not_a_class_of_row():
    notes = ifx.what_the_fragmentation_is(
        'ORDERS', 'E', [fragment('p1', 'a < 1'), fragment('p2', '')], ['dbs1', 'dbs2'])
    assert any('spread its I/O over devices' in note for note in notes)
    assert any('NOT carried over' in note and 'default tablespace' in note for note in notes)
    assert any('dbs1, dbs2' in note for note in notes)


def test_a_scheme_where_almost_every_row_sits_in_one_fragment_is_reported():
    """
    §4.2: a scheme where 95% of the rows sit in one fragment is not pruning anything, and the
    user should see that before reproducing it.
    """
    notes = ifx.what_the_fragmentation_is('ORDERS', 'E', [
        fragment('big', 'd < 1', 990000), fragment('small', '', 10)], ['dbs1'])
    note = [text for text in notes if 'sit in' in text]
    assert note and 'prunes nothing' in note[0]
    assert 'may be stale' in note[0]


def test_empty_fragments_are_counted():
    notes = ifx.what_the_fragmentation_is('ORDERS', 'E', [
        fragment('p1', 'd < 1', 100), fragment('p2', 'd < 2', 0),
        fragment('p3', 'd < 3', 0)], ['dbs1'])
    assert any('2 of the 3 fragments' in note and 'no rows at all' in note for note in notes)


def test_row_counts_nobody_gathered_are_reported_as_not_known():
    """P2-8 applied to a number: not gathered is not the same as zero."""
    notes = ifx.what_the_fragmentation_is('ORDERS', 'E', [fragment('p1', 'd < 1')], ['dbs1'])
    assert any('NOT known' in note and 'UPDATE STATISTICS' in note for note in notes)


def test_a_strategy_this_migrator_does_not_know_says_so_rather_than_guessing():
    notes = ifx.what_the_fragmentation_is('ORDERS', 'Z', [fragment('p1', 'a < 1')], ['dbs1'])
    assert any('does not know the name of' in note for note in notes)


def test_a_strategy_this_migrator_does_not_know_is_still_built_from_its_expressions():
    """
    The strategy letter decides the wording of the report and the expressions decide what can be
    built, so a release which spells one of them differently still migrates correctly.
    """
    scheme = scheme_of('Z', [fragment('p1', 'acct < 100'), fragment('p2', 'acct >= 100')])
    assert scheme['method'] == 'RANGE'


# --------------------------------------------------------------------------------------
# the connector


class Catalogue:
    """A cursor over the Informix system catalogue, answering by what the statement names."""

    def __init__(self, answers, raise_on=None):
        self.answers = answers
        self.raise_on = raise_on or ()
        self.rows = []
        self.statements = []

    def execute(self, statement, binds=None):
        self.statements.append(statement)
        for marker in self.raise_on:
            if marker in statement:
                raise Exception(f'-201: A syntax error has occurred ({marker})')
        for marker, rows in self.answers:
            if marker in statement:
                self.rows = rows
                return
        raise AssertionError(f'the test has no answer for this statement:\n{statement}')

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)

    def close(self):
        pass


def connector(answers, raise_on=None):
    made = InformixConnector.__new__(InformixConnector)
    made.config_parser = MagicMock()
    made.config_parser.convert_names_case = lambda name: (name or '').lower()
    made.messages = []
    made.config_parser.print_log_message.side_effect = \
        lambda level, message: made.messages.append((level, str(message)))
    made.connect = MagicMock()
    made.disconnect = MagicMock()
    made.connection = MagicMock()
    cursor = Catalogue(answers, raise_on)
    made.connection.cursor.return_value = cursor
    made.cursor = cursor
    return made


## strategy, evalpos, partition name, dbspace, nrows, exprtext
ORDERS = [('sysfragments f', [
    ('E', 0, 'orders_2022', 'dbs1', 5000, "order_date < DATE('2023-01-01')"),
    ('E', 1, 'orders_2023', 'dbs2', 6000, "order_date < DATE('2024-01-01')"),
    ('E', 2, 'orders_rest', 'dbs3', 100, None),
])]


def test_the_connector_reads_a_fragmentation_out_of_sysfragments():
    made = connector(ORDERS)
    scheme = made.fetch_table_partitioning(
        {'source_schema_name': 'app', 'source_table_name': 'ORDERS'})
    assert scheme['is_partitioned'] is True and scheme['is_partition'] is False
    assert scheme['method'] == 'RANGE' and scheme['columns'] == ['order_date']
    assert scheme['target_key_definition'] == 'RANGE ("order_date")'
    assert scheme['key_definition'] == 'FRAGMENT BY EXPRESSION (order_date)'
    assert scheme['partition_count'] == 3


def test_every_fragment_carries_both_spellings_and_its_row_count():
    made = connector(ORDERS)
    scheme = made.fetch_table_partitioning(
        {'source_schema_name': 'app', 'source_table_name': 'ORDERS'})
    first = scheme['partitions'][0]
    assert first['bound'] == "order_date < DATE('2023-01-01')"
    assert first['target_bound'] == "FOR VALUES FROM (MINVALUE) TO ('2023-01-01')"
    assert first['rows'] == 5000
    assert scheme['partitions'][2]['is_default'] is True
    assert scheme['partitions'][2]['target_bound'] == 'DEFAULT'


def test_the_dbspaces_are_recorded_and_reported_as_not_carried_over():
    made = connector(ORDERS)
    scheme = made.fetch_table_partitioning(
        {'source_schema_name': 'app', 'source_table_name': 'ORDERS'})
    assert scheme['engine_specific']['dbspaces'] == ['dbs1', 'dbs2', 'dbs3']
    assert scheme['engine_specific']['strategy_name'] == 'EXPRESSION'
    assert any('default tablespace' in note for note in scheme['notes'])


def test_a_round_robin_table_is_read_reported_and_refused():
    made = connector([('sysfragments f', [
        ('R', 0, 'p1', 'dbs1', 100, None), ('R', 1, 'p2', 'dbs2', 100, None)])])
    scheme = made.fetch_table_partitioning(
        {'source_schema_name': 'app', 'source_table_name': 'LOGS'})
    assert scheme['target_key_definition'] == ''
    assert any('ROUND ROBIN' in note for note in scheme['notes'])
    assert any('no partitioning key' in issue for issue in scheme['blockers'])


def test_a_table_which_is_not_fragmented_answers_nothing():
    made = connector([('sysfragments f', [])])
    assert made.fetch_table_partitioning(
        {'source_schema_name': 'app', 'source_table_name': 'CUSTOMERS'}) == {}


def test_a_fragment_with_no_name_of_its_own_is_given_one():
    """A fragment known only by the dbspace it sits in is not a name a relation could take."""
    made = connector([('sysfragments f', [
        ('E', 0, None, 'dbs1', 1, 'acct < 100'), ('E', 1, '', 'dbs2', 1, 'acct >= 100')])])
    scheme = made.fetch_table_partitioning(
        {'source_schema_name': 'app', 'source_table_name': 'ACCOUNTS'})
    assert [partition['name'] for partition in scheme['partitions']] == [
        'ACCOUNTS_p1', 'ACCOUNTS_p2']


def test_a_release_which_refuses_the_names_and_the_cast_still_reports_what_it_has():
    """
    `sysfragments.partition` is the fragment name of 11.70 and newer, and `exprtext` is a TEXT
    column which arrives as an object of the driver unless it is cast. A release which refuses
    either still has its strategy, its dbspaces and its row counts reported - which is most of
    what this source has to offer - and its fragments are named after their table.
    """
    made = connector([("f.evalpos, ''", [('E', 0, '', 'dbs1', 1, 'acct < 100')])],
                     raise_on=('f.partition',))
    scheme = made.fetch_table_partitioning(
        {'source_schema_name': 'app', 'source_table_name': 'ACCOUNTS'})
    assert scheme['partitions'][0]['bound'] == 'acct < 100'
    assert scheme['partitions'][0]['name'] == 'ACCOUNTS_p1'
    assert any(level == 'DEBUG' for level, _text in made.messages)


@pytest.mark.parametrize('value, expected', [
    ('acct < 100', 'acct < 100'),
    (b'acct < 100', 'acct < 100'),
    (None, ''),
])
def test_an_expression_is_decoded_however_the_driver_answered_it(value, expected):
    assert InformixConnector._fragment_expression_text(value) == expected


def test_a_clob_of_the_jdbc_driver_is_read_through_its_own_reader():
    class Clob:
        def length(self):
            return 10
        def getSubString(self, start, length):
            return 'acct < 100'[start - 1:start - 1 + length]
    assert InformixConnector._fragment_expression_text(Clob()) == 'acct < 100'


def test_the_fragmented_tables_of_a_schema_are_listed_in_one_query():
    made = connector([('sysfragments f', [('ORDERS',), ('LOGS',)])])
    assert made.fetch_partitioning_candidates('app') == {'ORDERS', 'LOGS'}
    assert len(made.cursor.statements) == 1


def test_a_catalogue_which_cannot_be_read_answers_nothing_and_says_so():
    made = connector(ORDERS, raise_on=('sysfragments f',))
    assert made.fetch_table_partitioning(
        {'source_schema_name': 'app', 'source_table_name': 'ORDERS'}) == {}
    assert any(level == 'ERROR' for level, _text in made.messages)


# --------------------------------------------------------------------------------------
# the facts of §4.4, out of the system catalogue


FACTS = [
    ('nrows FROM systables', [(400000,)]),
    ('c.colname, c.coltype', [
        ## Informix carries NOT NULL in bit 0x100 of coltype, and the type in the low byte
        ('order_id', 2 + 0x100, 4),
        ('order_date', 7 + 0x100, 4),
        ('shipped_at', 7, 4),
        ('note', 13, 100),
        ('document', 10 + 0x100, 8),
    ]),
    ('i.idxtype', [('orders_pk', 'P', 'U', 1, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0)]),
    ('c.colno, c.colname', [(1, 'order_id'), (2, 'order_date'), (3, 'shipped_at'),
                            (4, 'note'), (5, 'document')]),
    ('sysreferences r', [('order_item_fk', 'ORDER_ITEMS')]),
]


def facts_of(answers=None, raise_on=None):
    made = connector(answers or FACTS, raise_on)
    return made.fetch_partitioning_facts(
        {'source_schema_name': 'app', 'source_table_name': 'ORDERS'})


def test_the_facts_of_a_table_come_out_of_the_system_catalogue():
    facts = facts_of()
    assert facts['row_estimate'] == 400000
    assert facts['columns']['order_date']['type_name'] == 'DATE'
    assert facts['columns']['note']['type_name'] == 'VARCHAR'
    assert facts['date_range_types'] == ifx.DATE_RANGE_TYPES


def test_the_not_null_flag_is_the_bit_informix_keeps_it_in():
    facts = facts_of()
    assert facts['columns']['order_date']['not_null'] is True
    assert facts['columns']['shipped_at']['not_null'] is False


def test_informix_keeps_no_null_count_so_every_column_says_the_check_was_not_made():
    """
    P2-8: None is read by the checks as "not known" and reported as a check which was NOT made.
    Zero would say the column holds no NULL, which this source cannot answer at all.
    """
    assert all(column['null_fraction'] is None for column in facts_of()['columns'].values())


def test_the_primary_key_is_read_with_its_columns_in_order():
    keys = {key['name']: key for key in facts_of()['unique_keys']}
    assert keys['orders_pk']['is_primary'] is True
    assert keys['orders_pk']['columns'] == ['order_id']


def test_what_references_the_table_is_read():
    assert facts_of()['referenced_by'] == [{'name': 'order_item_fk', 'table': 'ORDER_ITEMS'}]


def test_informix_has_no_exclusion_constraint_and_no_table_inheritance():
    facts = facts_of()
    assert facts['exclusion_constraints'] == []
    assert facts['inherits_from_a_plain_table'] is False


def test_facts_which_cannot_be_read_are_answered_as_not_read():
    assert facts_of(raise_on=('c.colname, c.coltype',)) is None


# --------------------------------------------------------------------------------------
# the whole way through


def test_a_preserved_informix_table_is_built_from_the_translated_bounds():
    made = connector(ORDERS)
    scheme = made.fetch_table_partitioning(
        {'source_schema_name': 'app', 'source_table_name': 'ORDERS'})
    decision = partitioning.build_plan(
        {'ORDERS': scheme}, ['ORDERS'], mode_of=lambda name: 'preserve',
        target_version_num=160000)['ORDERS']
    assert decision.action == partitioning.PRESERVE
    assert decision.issues == []
    assert decision.target_key_definition == 'RANGE ("order_date")'
    assert [part.bound for part in decision.partitions] == [
        partition['target_bound'] for partition in scheme['partitions']]
    ## the source spelling survives as far as the report
    assert decision.partitions[0].source_bound.startswith('order_date <')


def test_what_the_fragmentation_costs_is_said_even_when_the_table_is_flattened():
    made = connector(ORDERS)
    scheme = made.fetch_table_partitioning(
        {'source_schema_name': 'app', 'source_table_name': 'ORDERS'})
    decision = partitioning.build_plan(
        {'ORDERS': scheme}, ['ORDERS'], mode_of=lambda name: 'flatten',
        target_version_num=160000)['ORDERS']
    assert decision.action == partitioning.FLATTEN
    assert any('spread its I/O over devices' in warning for warning in decision.warnings)


def test_a_round_robin_table_stops_only_a_run_which_would_build_it():
    made = connector([('sysfragments f', [
        ('R', 0, 'p1', 'dbs1', 100, None), ('R', 1, 'p2', 'dbs2', 100, None)])])
    scheme = made.fetch_table_partitioning(
        {'source_schema_name': 'app', 'source_table_name': 'LOGS'})
    preserved = partitioning.build_plan(
        {'LOGS': scheme}, ['LOGS'], mode_of=lambda name: 'preserve',
        target_version_num=160000)['LOGS']
    assert preserved.issues
    flattened = partitioning.build_plan(
        {'LOGS': scheme}, ['LOGS'], mode_of=lambda name: 'flatten',
        target_version_num=160000)['LOGS']
    assert flattened.issues == []
    ## and what the source really has is still said
    assert any('ROUND ROBIN' in warning for warning in flattened.warnings)


PARTITION_SQL = ('CREATE TABLE "{target_schema_name}"."{target_table_name}" PARTITION OF '
                 '"{target_schema_name}"."{parent_table_name}" {partition_bound}')


def test_the_create_table_of_a_preserved_informix_table_and_its_partitions():
    from credativ_pg_migrator.planner import Planner

    made = connector(ORDERS)
    scheme = made.fetch_table_partitioning(
        {'source_schema_name': 'app', 'source_table_name': 'ORDERS'})
    plan = partitioning.build_plan(
        {'ORDERS': scheme}, ['ORDERS'], mode_of=lambda name: 'preserve',
        target_version_num=160000)

    planner = Planner.__new__(Planner)
    planner.config_parser = MagicMock()
    planner.config_parser.convert_names_case = lambda name: (name or '').lower()
    planner.target_schema_name = 'migtest'
    target = MagicMock()
    target.get_create_partition_sql.side_effect = \
        lambda settings: PARTITION_SQL.format(**{
            key: (value or '').lower() if key.endswith('table_name') else value
            for key, value in settings.items()})
    planner.target_connection = target

    clause, statements = planner.partitioning_clause_for(plan['ORDERS'], 'orders')
    assert clause == ' PARTITION BY RANGE ("order_date")'
    assert statements == [
        'CREATE TABLE "migtest"."orders_2022" PARTITION OF "migtest"."orders" '
        "FOR VALUES FROM (MINVALUE) TO ('2023-01-01')",
        'CREATE TABLE "migtest"."orders_2023" PARTITION OF "migtest"."orders" '
        "FOR VALUES FROM ('2023-01-01') TO ('2024-01-01')",
        'CREATE TABLE "migtest"."orders_rest" PARTITION OF "migtest"."orders" DEFAULT',
    ]
