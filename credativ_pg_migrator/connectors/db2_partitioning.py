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
The partitioning of Db2, written the way PostgreSQL writes it.

`development/PARTITIONING_STRATEGY.md` §2.4 puts the Db2 family third, behind postgresql and
oracle, and calls it one family with one shape of answer: LUW reads a live catalogue, z/OS and
for i are DDL-only connectors whose parsers already matched the `PARTITION BY` clause out of the
`CREATE TABLE` text. All three share this module, the way they share `db2_query_conversion.py`,
and nothing in it imports a driver - so it can be tested on a machine with no Db2 client.

**The one bound which is not a bound.** Db2 writes `ENDING AT (x) INCLUSIVE` and means that x is
IN the partition; PostgreSQL's `TO (b)` means b is not. That is the same trap §2.4 records for
SQL Server's `RANGE LEFT`, and it is worse here because **INCLUSIVE is Db2's default** - a scheme
written `STARTING '2024-01-01' ENDING '2024-12-31'` has both ends inclusive, and copying the
bounds across gives a target which refuses every row of 31 December. So an inclusive upper bound
is converted to the exclusive one which means the same thing - the next value of the type - and
where the type has no next value the scheme is **refused** rather than moved by a day. A DATE has
one and an INTEGER has one; a DECIMAL with a scale, a VARCHAR and a TIMESTAMP do not, because
"the next value" of those depends on a precision the bound does not carry.

**Three mechanisms of Db2 which all say partition, and only one of which is one.** §2.1 and
§4.2:

  * **table partitioning by range** - `SYSCAT.DATAPARTITIONS`, or the `PARTITION BY RANGE`
    clause of the DDL. This is the one with a PostgreSQL counterpart, and it maps.
  * **DPF**, database partitioning - `SYSCAT.TABLES.PARTITION_MODE` - spreads the rows of a
    table over physical nodes by a hash of the distribution key. It is not table partitioning
    and PostgreSQL has nothing for it at all; what would replace it is a different product.
  * **MDC**, multi-dimensional clustering - a storage layout which decides which rows sit in a
    block together. The nearest PostgreSQL things are `CLUSTER` and a BRIN index, and neither of
    them is this.

A table can have all three at once, which is why each of them gets a sentence of its own rather
than one line saying the table is partitioned.

**Partition-by-growth**, which is z/OS's, has no key: a partition exists because the one before
it filled up. There is nothing to write into a `PARTITION BY` clause and it is refused.
"""

import re

from credativ_pg_migrator import partitioning


class UntranslatableScheme(Exception):
    """A Db2 scheme, or one bound of it, which PostgreSQL cannot be given as it stands."""


## The methods PostgreSQL has. Db2 for i also partitions by HASH, which maps in count only;
## everything else Db2 calls partitioning is one of the three mechanisms above.
TRANSLATED_METHODS = ('RANGE', 'HASH')

## What Db2 writes where a partition is open at one end, and what PostgreSQL writes for it. The
## catalogue may hold either the word or nothing at all, and the DDL holds the word.
MINVALUE = 'MINVALUE'
MAXVALUE = 'MAXVALUE'
UNBOUNDED_WORDS = (MINVALUE, MAXVALUE, '')

## The types whose values have a next one, so that `ENDING AT (x) INCLUSIVE` can be written as
## the `TO (x + 1)` which means the same thing. A DECIMAL with a scale, a VARCHAR, a TIMESTAMP
## and a FLOAT are deliberately absent: their next value depends on a precision the bound does
## not carry, and a bound moved by a guess is a partition which takes the wrong rows.
INTEGER_TYPES = ('SMALLINT', 'INTEGER', 'INT', 'BIGINT')
DECIMAL_TYPES = ('DECIMAL', 'DEC', 'NUMERIC', 'NUM')
DATE_TYPES = ('DATE',)

## The types a `date_range` can be counted in on a Db2 source.
DATE_RANGE_TYPES = ('DATE', 'TIMESTAMP')

## Db2 types which the migration gives a PostgreSQL type with no default operator class, so that
## a column of one cannot be a partition key whatever the entry says. XML becomes `xml`, which
## has neither a btree nor a hash class; the LOBs become `bytea` and `text`, which do have one
## and which Db2 itself refuses in a key.
TYPES_WITHOUT_AN_OPERATOR_CLASS = ('XML', 'DB2SECURITYLABEL')

## `PARTITION BY RANGE (col, col)`, `PARTITION BY HASH (col) INTO 8 PARTITIONS`, the older
## `PARTITION BY (col ASC)` of z/OS, and `PARTITION BY SIZE EVERY 4G`, which is
## partition-by-growth and has no key at all.
PARTITION_BY = re.compile(
    r"(?is)\bPARTITION\s+BY\s+(?:(RANGE|HASH)\s*)?(?:(SIZE)\b|\(\s*([^()]*?)\s*\))")

## `INTO 8 PARTITIONS` - the count of a Db2 for i hash scheme.
HASH_COUNT = re.compile(r"(?is)\bINTO\s+(\d+)\s+PARTITIONS?\b")

## One entry of the partition list. Db2 writes the ends in several spellings and leaves most of
## them out: `PARTITION 3 ENDING AT (199)`, `PARTITION q1 STARTING FROM ('a') INCLUSIVE ENDING
## AT ('b') EXCLUSIVE`, `STARTING '2024-01-01' ENDING '2024-12-31'`.
PARTITION_NAME = re.compile(r"(?is)^\s*PARTITION\s+(\"[^\"]+\"|[A-Z_0-9$#]+)\s*")
STARTING = re.compile(r"(?is)\bSTARTING\s*(?:FROM\s*)?(\(.*?\)|'(?:[^']|'')*'|[^\s,]+)"
                      r"(\s+INCLUSIVE|\s+EXCLUSIVE)?")
ENDING = re.compile(r"(?is)\bENDING\s*(?:AT\s*)?(\(.*?\)|'(?:[^']|'')*'|[^\s,]+)"
                    r"(\s+INCLUSIVE|\s+EXCLUSIVE)?")

## `EVERY 1 MONTH`, `EVERY 100` - Db2 generates the partitions from one entry. It is not
## expanded here: `target_partitioning` with a `date_range` is the thing which generates a
## calendar of partitions, it does it from the data rather than from a clause, and a second
## generator which reads a Db2 interval would be a worse copy of it.
EVERY = re.compile(r"(?is)\bEVERY\b")


def unquote(value):
    """The value inside a Db2 catalogue or DDL bound, with its wrapping taken off."""
    text = (value or '').strip()
    while text.startswith('(') and text.endswith(')'):
        text = text[1:-1].strip()
    return text


def is_unbounded(value):
    """Whether a bound is one of Db2's open ends - MINVALUE, MAXVALUE, or nothing at all."""
    return unquote(value).upper() in UNBOUNDED_WORDS


def to_postgresql_value(value):
    """
    One Db2 bound value, written as PostgreSQL writes it.

    Db2 and PostgreSQL spell a literal the same way - the single quote is the delimiter and one
    inside it is written twice - so there is nothing to translate in the value itself. What
    there is to do is take off the brackets Db2 wraps a bound in and refuse what is not a
    literal at all: `CURRENT DATE` in a bound would have to be evaluated, and evaluating it
    would give the scheme a boundary which moves.
    """
    text = unquote(value)
    if not text:
        raise UntranslatableScheme('the source holds no value for this bound')
    upper = text.upper()
    if upper in (MINVALUE, MAXVALUE):
        return upper
    if re.match(r"(?is)^'(?:[^']|'')*'$", text):
        return text
    if re.match(r"(?is)^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$", text):
        return text
    raise UntranslatableScheme(
        f"the bound {text} is not a literal, so it cannot be written as a PostgreSQL bound "
        f"without asking Db2 to evaluate it")


def successor(value, type_name):
    """
    The next value of a type after this one - what an INCLUSIVE upper bound of Db2 has to become
    for PostgreSQL, whose upper bound is always exclusive.

    Only where the type really has a next value. A DECIMAL with a scale, a VARCHAR, a TIMESTAMP
    and a FLOAT do not have one that a bound written without a precision could name, and a bound
    moved by a guess is a partition which takes rows belonging to the one beside it.
    """
    plain = base_type_of(type_name)
    text = unquote(value)
    ## the arithmetic itself is `partitioning.next_discrete_value()` - Sybase ASE writes
    ## `VALUES <= (x)` and means the same thing Db2 means by INCLUSIVE, so the two share it and
    ## what differs is only which type names of each engine count as a date or a whole number
    kind = ''
    if plain in DATE_TYPES:
        kind = partitioning.DISCRETE_DATE
    elif plain in INTEGER_TYPES or (plain in DECIMAL_TYPES and scale_of(type_name) == 0):
        kind = partitioning.DISCRETE_INTEGER
    if kind:
        try:
            return partitioning.next_discrete_value(text, kind)
        except ValueError:
            raise UntranslatableScheme(
                f"the inclusive bound {text} is not a {plain} this migrator can read, so the "
                f"exclusive bound PostgreSQL needs - the value after it - cannot be worked out")
    raise UntranslatableScheme(
        f"the partition ends AT {text} INCLUSIVE, and PostgreSQL's upper bound is always "
        f"exclusive. Converting it needs the next value of {type_name or 'the column type'}, "
        f"which only a DATE and a whole number have - a {type_name or 'type'} bound moved by a "
        f"guess is a partition which takes the rows of the one beside it. Write the partitions "
        f"out with target_partitioning, or set source_partitioning: flatten for this table")


def base_type_of(type_name):
    """The name of a type without the precision and scale Db2 writes after it."""
    return re.sub(r'\s*\([^)]*\)', '', str(type_name or '')).strip().upper()


def scale_of(type_name):
    """The scale of a DECIMAL(p,s), or 0 where none is written - which is Db2's own default."""
    match = re.search(r'\(\s*\d+\s*(?:,\s*(\d+)\s*)?\)', str(type_name or ''))
    if not match:
        return 0
    return int(match.group(1)) if match.group(1) else 0


def range_bound(low, low_inclusive, high, high_inclusive, type_name):
    """
    `FOR VALUES FROM (…) TO (…)` for one Db2 range partition.

    low, high         - the ends as Db2 holds them, or None / MINVALUE / MAXVALUE for an open one
    low_inclusive,
    high_inclusive    - whether Db2 counts the end itself as in the partition. Db2's default is
                        INCLUSIVE for both, PostgreSQL's is inclusive below and exclusive above,
                        so the upper end is the one which has to be moved
    type_name         - the type of the partitioning column, which is what says whether it can be
    """
    lower = MINVALUE if is_unbounded(low) else to_postgresql_value(low)
    if lower != MINVALUE and not low_inclusive:
        ## `STARTING FROM (x) EXCLUSIVE` - PostgreSQL's FROM is always inclusive, so the
        ## partition really starts at the value after x
        lower = successor(lower, type_name)

    upper = MAXVALUE if is_unbounded(high) else to_postgresql_value(high)
    if upper != MAXVALUE and high_inclusive:
        upper = successor(upper, type_name)

    return f"FOR VALUES FROM ({lower}) TO ({upper})"


def hash_bound(position, count):
    """
    `FOR VALUES WITH (MODULUS n, REMAINDER i)` for the i-th of n hash partitions of Db2 for i.

    The count is carried over and the placement is not: Db2 hashes with its own function and
    PostgreSQL with its own. Nothing is lost by it - the rows go in through the parent and the
    target routes each of them - and the caller says so, because a reader would assume otherwise.
    """
    if not count or count < 1:
        raise UntranslatableScheme('a hash scheme with no partitions')
    if position is None or position < 0 or position >= count:
        raise UntranslatableScheme(
            f"the hash partition at position {position} of {count} is not one this scheme has")
    return f"FOR VALUES WITH (MODULUS {count}, REMAINDER {position})"


def key_definition(method, columns, name_of_column):
    """
    The `PARTITION BY` clause of the target, in the names the target will have.

    Db2 holds `SALES_DATE` and `names_case_handling` may give the target `sales_date`, and an
    unquoted copy of the first names a column which is not there.
    """
    if method not in TRANSLATED_METHODS:
        raise UntranslatableScheme(
            f"PostgreSQL partitions by RANGE, LIST and HASH; Db2's {method or 'scheme'} has no "
            f"counterpart among them")
    if not columns:
        raise UntranslatableScheme(f"a {method} scheme whose key columns the source does not hold")
    written = ', '.join(f'"{name_of_column(column)}"' for column in columns)
    return f"{method} ({written})"


## ------------------------------------------------------------------------------------
## The DDL half: what the two file-based connectors have in their CREATE TABLE text.
##
## `ibm_db2_zos_connector` and `ibm_db2_i_connector` never connect to a source - the structure
## comes out of `.sql` extracts - so the clause itself is the catalogue. §2.4 records that both
## parsers already matched it and wrote it into the table COMMENT; what was missing is reading
## it as a scheme.
## ------------------------------------------------------------------------------------


def parse_partition_clause(text):
    """
    The `PARTITION BY …` of a `CREATE TABLE`, as far as the target has to know it.

    Returns {'method', 'columns', 'ranges', 'hash_count'} - and an empty method for a table
    with no clause. `ranges` is the text of the partition list, kept as it stands so that the
    protocol holds what the DDL said; `parse_partition_list()` reads it.

    The four spellings this has to know, and the two which are not RANGE:

        PARTITION BY RANGE (sales_date) (PARTITION q1 STARTING '2024-01-01' ENDING '2024-03-31')
        PARTITION BY (ACCT_NUM ASC) (PARTITION 1 ENDING AT (199))      -- older z/OS
        PARTITION BY HASH (customer_id) INTO 8 PARTITIONS              -- Db2 for i
        PARTITION BY SIZE EVERY 4G                                     -- partition-by-growth
    """
    found = PARTITION_BY.search(text or '')
    if not found:
        return {'method': '', 'columns': [], 'ranges': '', 'hash_count': None}

    if found.group(2):
        ## PARTITION BY SIZE EVERY nG - partition-by-growth, which has no key
        return {'method': 'SIZE', 'columns': [], 'ranges': '', 'hash_count': None}

    ## `PARTITION BY (col ASC)` with no method written is the older z/OS spelling of RANGE
    method = (found.group(1) or 'RANGE').upper()
    columns = [re.sub(r'(?is)\s+(ASC|DESC|NULLS\s+(FIRST|LAST))\b', '', column).strip().strip('"')
               for column in partitioning.split_top_level_commas(found.group(3) or '')]
    columns = [column for column in columns if column]

    trailing = (text or '')[found.end():]
    if method == 'HASH':
        ## `ranges` holds what the DDL wrote about the partitions, whatever the method - the
        ## list for a RANGE scheme and the count for a HASH one. What it means is read with the
        ## method beside it, which is why the two are stored together
        count = HASH_COUNT.search(trailing)
        return {'method': method, 'columns': columns,
                'ranges': count.group(0).strip() if count else '',
                'hash_count': int(count.group(1)) if count else None}

    return {'method': method, 'columns': columns,
            'ranges': partition_list_text(trailing), 'hash_count': None}


def partition_list_text(trailing):
    """
    The text inside the brackets which hold the partition list, brackets balanced.

    The list is full of brackets of its own - `ENDING AT (199)` - so it cannot be taken with a
    regular expression which stops at the first closing one, which is how the parser this
    replaces read `PARTITION 1 ENDING AT (199), PARTITION 2 ENDING AT (299)` as one partition.
    """
    start = trailing.find('(')
    if start == -1:
        return ''
    depth = 0
    for index in range(start, len(trailing)):
        if trailing[index] == '(':
            depth += 1
        elif trailing[index] == ')':
            depth -= 1
            if depth == 0:
                return trailing[start + 1:index].strip()
    return ''


def parse_partition_list(ranges, method='RANGE'):
    """
    The partitions of a Db2 range scheme, out of the text of its list.

    Each of them answers the same four things `SYSCAT.DATAPARTITIONS` answers for a live LUW
    database, so that the three connectors of the family hand the same structure on:
    {'name', 'low', 'low_inclusive', 'high', 'high_inclusive'}.

    A partition with no STARTING - which is z/OS's ordinary spelling - begins where the one
    before it ended, and the first one at MINVALUE. That is Db2's rule and not a guess: the
    ranges of a partitioned table space are contiguous and ordered by partition number.
    """
    text = (ranges or '').strip()
    if not text:
        return []
    if EVERY.search(text):
        raise UntranslatableScheme(
            "the partition list is written with EVERY, so Db2 generates the partitions from one "
            "entry rather than listing them. This migrator does not expand it - write the scheme "
            "with target_partitioning, whose date_range generates a calendar of partitions from "
            "the data itself")

    partitions = []
    previous_high, previous_high_inclusive = None, True
    for entry in partitioning.split_top_level_commas(text):
        entry = entry.strip()
        if not entry:
            continue
        name = ''
        named = PARTITION_NAME.match(entry)
        if named:
            name = named.group(1).strip('"')
            entry = entry[named.end():]

        starting = STARTING.search(entry)
        ending = ENDING.search(entry)
        if starting:
            low = starting.group(1)
            low_inclusive = (starting.group(2) or '').strip().upper() != 'EXCLUSIVE'
        elif previous_high is None:
            ## the first partition of a list which writes only its ends is open below
            low, low_inclusive = MINVALUE, True
        else:
            ## it starts where the one before it stopped: at the value after an inclusive end,
            ## and at the value itself after an exclusive one
            low, low_inclusive = previous_high, not previous_high_inclusive
        if not ending:
            raise UntranslatableScheme(
                f"the partition {name or len(partitions) + 1} of this list has no ENDING, so "
                f"where it stops is not written anywhere this migrator can read")
        high = ending.group(1)
        high_inclusive = (ending.group(2) or '').strip().upper() != 'EXCLUSIVE'

        partitions.append({'name': name, 'low': low, 'low_inclusive': low_inclusive,
                           'high': high, 'high_inclusive': high_inclusive})
        previous_high, previous_high_inclusive = high, high_inclusive
    return partitions


def partition_name_for(table_name, index, written_name):
    """
    What a partition of the target is called.

    Db2 for z/OS numbers its partitions rather than naming them - `PARTITION 3 ENDING AT (…)` -
    and a target relation cannot be called `3`, so a numbered partition is given the name of its
    table and its number. A named one keeps its name.
    """
    name = (written_name or '').strip()
    if name and not name.isdigit():
        return name
    return f"{table_name}_p{name or index}"


def scheme_from_ddl(table_name, method, columns, ranges, column_types, name_of_column):
    """
    What `fetch_table_partitioning()` answers for a table of a DDL-only Db2 connector.

    The two file-based connectors - z/OS and for i - never see a catalogue: the `PARTITION BY`
    clause of the `CREATE TABLE` text is the catalogue, and their parsers put it into
    `ddl_tables` while they read the extract. This turns those three fields into the same
    structure the live LUW connector answers, so that everything behind them - the plan, the
    report, the protocol, the statements - is the same code for all three.

    table_name      - the table, for the names generated partitions are given
    method          - RANGE, HASH, SIZE, or empty
    columns         - the partitioning columns
    ranges          - the partition list for RANGE, the `INTO n PARTITIONS` for HASH
    column_types    - {column: type}, which decides whether an INCLUSIVE bound can be converted
    name_of_column  - names_case_handling, for the key the target is given
    """
    method = (method or '').strip().upper()
    columns = [column for column in (columns or []) if column]
    notes = []
    blockers = []

    if method == 'SIZE':
        ## partition-by-growth: a partition exists because the one before it filled up. There is
        ## no key, so there is nothing to write into a PARTITION BY clause
        return {
            'is_partitioned': True, 'is_partition': False, 'method': 'SIZE', 'columns': [],
            'key_definition': 'PARTITION BY SIZE (partition-by-growth)',
            'target_key_definition': '', 'level': 1, 'levels_below': [], 'partitions': [],
            'partition_count': 0, 'engine_specific': {'partitioning_type': 'SIZE'},
            'notes': [f"{table_name} is partitioned BY SIZE on the source - partition-by-growth, "
                      f"where a partition exists because the one before it filled up. The "
                      f"partitions have no key and no bounds, so there is nothing PostgreSQL "
                      f"could be given: PostgreSQL routes a row by its value and Db2 routes it "
                      f"by which partition still has room"],
            'blockers': [f"{table_name} is partitioned BY SIZE on the source and the same scheme "
                         f"cannot be built on PostgreSQL: partition-by-growth has no partitioning "
                         f"key at all. Set source_partitioning: flatten for this table, or write "
                         f"a scheme of your own with target_partitioning"],
        }

    partitions = []
    if method == 'HASH':
        found = HASH_COUNT.search(ranges or '')
        count = int(found.group(1)) if found else 0
        for index in range(count):
            partitions.append({'name': partition_name_for(table_name, index + 1, ''),
                               'bound': f"PARTITION {index + 1} OF {count} BY HASH",
                               'target_bound': hash_bound(index, count),
                               'is_default': False, 'is_partitioned': False, 'rows': None})
        if not count:
            blockers.append(
                f"{table_name} is partitioned BY HASH on the source and the DDL does not say "
                f"into how many partitions, so the scheme cannot be built. Write it with "
                f"target_partitioning, or set source_partitioning: flatten for this table")
        else:
            notes.append(
                f"{table_name} is partitioned by HASH into {count} on the source, and the target "
                f"is given {count} hash partitions over the same column - but Db2 hashes with "
                f"its own function and PostgreSQL with its own, so a row which sat in one "
                f"partition on Db2 sits in another here. Nothing is lost by it: the rows go in "
                f"through the parent and the target routes each of them")
    elif method == 'RANGE':
        type_name = column_types.get(columns[0], '') if columns else ''
        try:
            written = parse_partition_list(ranges, method)
        except UntranslatableScheme as e:
            written = []
            blockers.append(f"the partition list of {table_name} cannot be read: {e}")
        if not written and not blockers:
            blockers.append(
                f"{table_name} carries a PARTITION BY RANGE clause whose partition list the DDL "
                f"does not hold, so the target would be created partitioned with nothing under "
                f"it - a table which refuses every row. Set source_partitioning: flatten for "
                f"this table")
        inclusive_ends = 0
        for index, entry in enumerate(written, start=1):
            partition = {'name': partition_name_for(table_name, index, entry['name']),
                         'bound': bound_text_of(entry), 'target_bound': '',
                         'is_default': False, 'is_partitioned': False, 'rows': None}
            partitions.append(partition)
            if len(columns) > 1:
                continue
            try:
                partition['target_bound'] = range_bound(
                    entry['low'], entry['low_inclusive'],
                    entry['high'], entry['high_inclusive'], type_name)
                if entry['high_inclusive'] and not is_unbounded(entry['high']):
                    inclusive_ends += 1
            except UntranslatableScheme as e:
                blockers.append(
                    f"the partition {partition['name']} of {table_name} cannot be given to "
                    f"PostgreSQL as it stands: {e}")
        if len(columns) > 1:
            blockers.append(
                f"{table_name} is partitioned by ({', '.join(columns)}) on the source. This "
                f"migrator carries over a Db2 range scheme over ONE column: a bound of more than "
                f"one has to be converted end by end, and the type of each of them decides "
                f"whether it can be. Write the scheme with target_partitioning, or set "
                f"source_partitioning: flatten for this table")
        if inclusive_ends and not blockers:
            notes.append(
                f"{inclusive_ends} partition(s) of {table_name} end AT a value INCLUSIVE, which "
                f"is Db2's default and which PostgreSQL cannot say - its upper bound is always "
                f"exclusive. Each of them is carried over as the exclusive bound which holds the "
                f"same rows: ENDING AT '2024-12-31' INCLUSIVE becomes TO ('2025-01-01'). The "
                f"partitions hold what they held")
        if written and not blockers and not is_unbounded(written[-1]['high']):
            notes.append(
                f"the partitions of {table_name} end at {partitions[-1]['name']}, whose bound is "
                f"{partitions[-1]['target_bound']}. The source has no MAXVALUE partition and the "
                f"target is given none either, so a row past that bound is refused - which is "
                f"what the source does today as well. Write a target_partitioning entry with a "
                f"future: window if the migration is the moment to change that")

    target_key_definition = ''
    try:
        target_key_definition = key_definition(method, columns, name_of_column)
    except UntranslatableScheme as e:
        blockers.append(
            f"{table_name} is partitioned by {method or 'a method the DDL does not name'} on the "
            f"source and the same scheme cannot be built on PostgreSQL: {e}. Set "
            f"source_partitioning: flatten for this table, or write a scheme of your own with "
            f"target_partitioning")

    return {
        'is_partitioned': True,
        ## a data partition of Db2 is not a table of the schema, and the DDL extract holds none
        'is_partition': False,
        'parent_table': '', 'parent_schema': '', 'partition_bound': '',
        'method': method,
        'columns': columns,
        'key_definition': f"{method} ({', '.join(columns)})" if columns else method,
        'target_key_definition': target_key_definition,
        'level': 1,
        'levels_below': [],
        'partitions': partitions,
        'partition_count': len(partitions),
        'engine_specific': {'partitioning_type': method, 'clause': ranges or ''},
        'notes': notes,
        'blockers': blockers,
    }


def bound_text_of(entry):
    """One partition of a Db2 list, written back the way the DDL says it - for the report."""
    low = unquote(entry['low']) or MINVALUE
    high = unquote(entry['high']) or MAXVALUE
    return (f"STARTING FROM {low} {'INCLUSIVE' if entry['low_inclusive'] else 'EXCLUSIVE'} "
            f"ENDING AT {high} {'INCLUSIVE' if entry['high_inclusive'] else 'EXCLUSIVE'}")
