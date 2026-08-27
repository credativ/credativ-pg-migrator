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
What becomes of a table which is partitioned - decided once, before anything is created.

`development/PARTITIONING_STRATEGY.md` §4 and §2.4. Three things a migration can do with a
partitioned source table, and the configuration says which:

  * **preserve** - the same scheme on the target. Every partition of the source becomes a
    partition of the target, sub-partitions and all, and the data goes in through the parent so
    that PostgreSQL routes each row to the partition it belongs in.
  * **flatten**  - one ordinary table. The scheme is dropped, which is a change the run has to
    say out loud rather than perform quietly.
  * **repartition** - a scheme the source never had, from `target_partitioning`. It wins over
    both of the above, because it is the one the user wrote out by hand.

And a fourth outcome which is not a choice: a **partition** of a table which is being migrated
is not a table of its own. It is created with its parent and its rows arrive through it, so it
is left out of the table list entirely - and a run which migrated it separately would create
every row twice and would try to attach a partition to a parent which is not partitioned.

Everything here is a pure function of the data it is given: no connection, no configuration
object, no clock. That is what makes the decision testable without a database, which is what
`development/PARTITIONING_STRATEGY.md` §9 asks for.
"""

import datetime
import re

## What a table is doing in this migration.
NOT_PARTITIONED = 'not partitioned'
PRESERVE = 'preserve'
FLATTEN = 'flatten'
REPARTITION = 'repartition'
PART_OF_PARENT = 'part of parent'
ORPHAN_PARTITION = 'orphan partition'

## The actions under which the table is not migrated as a table of its own.
NOT_A_TABLE_OF_ITS_OWN = (PART_OF_PARENT,)

## The server version each method needs. Declarative partitioning arrived in PostgreSQL 10,
## hash partitioning and the DEFAULT partition in 11.
METHOD_VERSIONS = {'RANGE': 100000, 'LIST': 100000, 'HASH': 110000}
DEFAULT_PARTITION_VERSION = 110000

## PostgreSQL refuses an identifier longer than this, and truncates the rest of it silently -
## which turns two partitions of a long table into one name and a collision. It is checked
## against a name this migrator generates and against one it carries over from a source whose
## own limit is higher: Oracle allows 128 bytes since 12.2.
MAX_IDENTIFIER_LENGTH = 63


def version_text(version_num):
    """A PostgreSQL server version number as the number a person reads."""
    if not version_num:
        return 'unknown'
    return str(version_num // 10000)


## The kinds of value which have a next one, so that a bound written INCLUSIVE by a source can
## be given to PostgreSQL, whose upper bound is always exclusive. Everything else - a decimal
## with a scale, a text, a timestamp, a float - has no next value a bound written without a
## precision could name.
DISCRETE_DATE = 'date'
DISCRETE_INTEGER = 'integer'


def next_discrete_value(value, kind):
    """
    The value after this one, for the two kinds which have one.

    Db2 says `ENDING AT (x) INCLUSIVE` and Sybase ASE says `VALUES <= (x)`, and both mean that x
    is IN the partition; PostgreSQL's `TO (b)` means b is not. Converting the one into the other
    is the same arithmetic for both, so it stands here once - what differs between the two
    sources is only which of their type names count as a date and which as a whole number.

    Raises ValueError where the value is not one this kind can count in; the caller turns that
    into the refusal its own source needs to word.
    """
    text = str(value or '').strip().strip("'")
    if kind == DISCRETE_DATE:
        day = datetime.date.fromisoformat(text)
        return "'" + (day + datetime.timedelta(days=1)).isoformat() + "'"
    if kind == DISCRETE_INTEGER:
        return str(int(text) + 1)
    raise ValueError(f"{kind or 'this type'} has no next value")


def split_top_level_commas(text):
    """
    Split on the commas which are not inside brackets or a string literal.

    Every catalogue which writes a partitioning key or a bound writes more than one of them as
    a comma-separated list, and every one of them can hold a comma which is not a separator -
    `RANGE (date_trunc('month'::text, created_at))` in pg_get_partkeydef(), and
    `10, TO_DATE(' 2024-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS')` in Oracle's HIGH_VALUE.
    Reading those as two values answers columns and bounds which do not exist, so the split
    lives here, once, rather than in each connector which needs it.
    """
    parts = []
    depth = 0
    in_literal = False
    current = []
    index = 0
    while index < len(text):
        char = text[index]
        if in_literal:
            current.append(char)
            if char == "'":
                if index + 1 < len(text) and text[index + 1] == "'":
                    current.append(text[index + 1])
                    index += 2
                    continue
                in_literal = False
            index += 1
            continue
        if char == "'":
            in_literal = True
            current.append(char)
        elif char in '([':
            depth += 1
            current.append(char)
        elif char in ')]':
            depth -= 1
            current.append(char)
        elif char == ',' and depth == 0:
            parts.append(''.join(current))
            current = []
        else:
            current.append(char)
        index += 1
    parts.append(''.join(current))
    return parts


class Partition:
    """One partition which has to be created, and everything needed to create it."""

    __slots__ = ('name', 'parent', 'bound', 'key_definition', 'level', 'is_default',
                 'start', 'end', 'source_bound')

    def __init__(self, name, parent, bound, key_definition='', level=2, is_default=False,
                 start=None, end=None, source_bound=''):
        self.name = name
        self.parent = parent
        ## what the target is given - already PostgreSQL, whatever the source wrote
        self.bound = bound
        ## set when the partition is itself partitioned - the scheme has more than one level
        self.key_definition = key_definition
        self.level = level
        self.is_default = is_default
        ## the dates a GENERATED partition was built from, so that a report can say what the
        ## scheme covers without parsing the bound it has just written
        self.start = start
        self.end = end
        ## what the source wrote, where the two spellings differ - `VALUES LESS THAN
        ## (TO_DATE(…))` beside the `FOR VALUES FROM … TO …` it became. The protocol keeps
        ## both, so that a reader can see what was translated into what
        self.source_bound = source_bound or bound

    def __repr__(self):
        return f'Partition({self.name!r} of {self.parent!r} {self.bound!r})'


class TableDecision:
    """What was decided for one table, and everything the reader of a report needs."""

    __slots__ = ('table_name', 'action', 'reason', 'scheme', 'partitions', 'issues',
                 'warnings', 'root_table')

    def __init__(self, table_name, action, reason='', scheme=None, root_table=''):
        self.table_name = table_name
        self.action = action
        self.reason = reason
        self.scheme = scheme or {}
        self.partitions = []
        ## blocking: the migration would fail later, so it is stopped now
        self.issues = []
        ## worth saying, and not a reason to stop
        self.warnings = []
        ## the top of the tree, for a partition
        self.root_table = root_table

    @property
    def migrated_as_table(self):
        """Whether this table gets a row of its own in the table list of the migration."""
        return self.action not in NOT_A_TABLE_OF_ITS_OWN

    @property
    def key_definition(self):
        """The key as the SOURCE writes it - which is what a report shows."""
        return self.scheme.get('key_definition', '')

    @property
    def target_key_definition(self):
        """
        The key the target is given, which is the same thing for a PostgreSQL source and is
        not for any other: Oracle holds `ORDER_DATE` and the target may hold `order_date`, and
        an unquoted RANGE (ORDER_DATE) in the CREATE TABLE of the target names a column which
        is not there.
        """
        return self.scheme.get('target_key_definition') or self.key_definition

    @property
    def method(self):
        return self.scheme.get('method', '')

    @property
    def source_level_count(self):
        """
        How many levels the scheme of the source has - the number §4.2's headline reports as
        "4 of them have more than one level".

        Two sources answer it two ways, and both are right. A connector whose partitions are
        relations of their own - postgresql - answers one level per call and the walk finds the
        rest, so the depth is in the partitions the plan collected. A connector whose
        sub-partitions are not relations at all - oracle - names them in `levels_below`, because
        there is no walk which could find them.
        """
        if not self.scheme.get('is_partitioned'):
            return 0
        below = self.scheme.get('levels_below') or []
        if below:
            return 1 + len(below)
        ## a Partition of the first level is level 2 - the parent is level 1
        return max([partition.level for partition in self.partitions] or [2]) - 1

    @property
    def target_level_count(self):
        """
        How many levels the scheme the TARGET is given has - which is not always as many as the
        source had. §2.2: an Oracle composite arrives one level deep on purpose.
        """
        if self.action == REPARTITION:
            return 1
        if self.action != PRESERVE:
            return 0
        return max([partition.level for partition in self.partitions] or [2]) - 1

    def describe(self):
        """One line for the report."""
        if self.action == NOT_PARTITIONED:
            return 'not partitioned'
        if self.action == PART_OF_PARENT:
            return f"partition of {self.root_table} - created with it"
        if self.action == ORPHAN_PARTITION:
            return f"partition of {self.root_table}, which is not migrated - created as an ordinary table"
        if self.action == PRESERVE:
            return (f"{self.key_definition} - preserved, "
                    f"{len(self.partitions)} partition(s)")
        if self.action == FLATTEN:
            return (f"{self.key_definition} on the source - FLATTENED into one table, "
                    f"{self.scheme.get('partition_count', 0)} partition(s) dropped")
        if self.action == REPARTITION:
            return 'partitioned by target_partitioning'
        return self.action


def root_of(table_name, schemes, seen=None):
    """
    The table at the top of the partitioning tree `table_name` belongs to.

    A partition of a partition resolves through as many levels as there are. A tree which
    points at itself - which no catalogue produces and a fixture can - stops rather than
    looping.
    """
    seen = seen or set()
    current = table_name
    while True:
        scheme = schemes.get(current) or {}
        parent = scheme.get('parent_table') if scheme.get('is_partition') else None
        if not parent or parent in seen:
            return current
        seen.add(current)
        current = parent


def descendants_of(table_name, schemes, level=2, seen=None):
    """
    Every partition below `table_name`, parents before their own children, so that the list
    can be executed in order.
    """
    seen = seen if seen is not None else set()
    found = []
    scheme = schemes.get(table_name) or {}
    for partition in scheme.get('partitions') or []:
        name = partition.get('name')
        if not name or name in seen:
            continue
        seen.add(name)
        child_scheme = schemes.get(name) or {}
        found.append(Partition(
            name=name,
            parent=table_name,
            ## a source which does not write its bounds the way PostgreSQL does answers both,
            ## and the target is given the translation - see the contract in
            ## DatabaseConnector.fetch_table_partitioning()
            bound=partition.get('target_bound') or partition.get('bound', ''),
            source_bound=partition.get('bound', ''),
            ## a partition which is itself partitioned carries its own key, and its children
            ## come behind it
            key_definition=child_scheme.get('key_definition', '') if partition.get('is_partitioned') else '',
            level=level,
            is_default=bool(partition.get('is_default')),
        ))
        if partition.get('is_partitioned'):
            found.extend(descendants_of(name, schemes, level + 1, seen))
    return found


def build_plan(schemes, selected_tables, mode_of, repartitioned_tables=(),
               target_version_num=None):
    """
    What happens to every selected table, decided in one place.

    schemes             - {table_name: what fetch_table_partitioning() answered}
    selected_tables     - the tables the configuration selects, in the spelling of the source
    mode_of             - callable(table_name) -> 'preserve' | 'flatten'
    repartitioned_tables- the tables `target_partitioning` names
    target_version_num  - the server version of the target, for the checks which need one

    Returns {table_name: TableDecision}.
    """
    selected = list(selected_tables)
    selected_set = set(selected)
    repartitioned = {name for name in repartitioned_tables}
    plan = {}

    for table_name in selected:
        scheme = schemes.get(table_name) or {}

        ## a partition is not a table of its own - unless the table it belongs to is not being
        ## migrated at all, and then it is the only thing left of it
        if scheme.get('is_partition'):
            root = root_of(table_name, schemes)
            if root in selected_set and root != table_name and root not in repartitioned:
                decision = TableDecision(table_name, PART_OF_PARENT, scheme=scheme, root_table=root)
                decision.reason = (f"it is a partition of {root}, which is migrated - it is "
                                   f"created with its parent and its rows arrive through it")
                plan[table_name] = decision
                continue
            decision = TableDecision(table_name, ORPHAN_PARTITION, scheme=scheme, root_table=root)
            decision.reason = (f"it is a partition of {root}, which this migration does not "
                               f"migrate as a partitioned table")
            decision.warnings.append(
                f"{table_name} is a partition of {root} on the source. {root} is not migrated "
                f"as a partitioned table, so {table_name} is created as an ordinary table "
                f"holding the rows of that one partition. Select {root} as well to keep the "
                f"scheme, or say so on purpose")
            plan[table_name] = decision
            continue

        if table_name in repartitioned:
            decision = TableDecision(table_name, REPARTITION, scheme=scheme)
            if scheme.get('is_partitioned'):
                decision.warnings.append(
                    f"{table_name} is partitioned on the source ({scheme.get('key_definition')}) "
                    f"and target_partitioning names it as well. The entry of the configuration "
                    f"wins - it is the scheme somebody wrote out by hand - and the scheme of the "
                    f"source is not carried over")
            plan[table_name] = decision
            continue

        if not scheme.get('is_partitioned'):
            plan[table_name] = TableDecision(table_name, NOT_PARTITIONED, scheme=scheme)
            continue

        mode = mode_of(table_name)
        if mode == FLATTEN:
            decision = TableDecision(table_name, FLATTEN, scheme=scheme)
            decision.reason = 'source_partitioning: flatten'
            decision.warnings.append(
                f"{table_name} is partitioned on the source - {scheme.get('key_definition')}, "
                f"{scheme.get('partition_count', 0)} partition(s) - and is created as ONE "
                f"ordinary table. Nothing of the scheme is carried over: no pruning, no "
                f"DETACH, and every index is one index over the whole table")
            plan[table_name] = decision
            continue

        decision = TableDecision(table_name, PRESERVE, scheme=scheme)
        decision.reason = 'source_partitioning: preserve'
        decision.partitions = descendants_of(table_name, schemes)
        plan[table_name] = decision

    _check(plan, schemes, selected_set, target_version_num)
    return plan


def _check(plan, schemes, selected_set, target_version_num):
    """
    The feasibility of every decision, against what the target can do.

    Everything here is answerable before a single object is created, and every one of them is
    a run which otherwise fails somewhere in the middle.
    """
    for table_name, decision in plan.items():
        ## what the connector found about the scheme of the source and the reader has to be
        ## told whatever becomes of the table - a mechanism with no counterpart, a level which
        ## is not carried over, a global index PostgreSQL cannot have. A note is a fact about
        ## the source; a blocker is a reason the same scheme cannot be BUILT, and it applies
        ## only where it would be.
        decision.warnings.extend(decision.scheme.get('notes') or [])
        if decision.action == PRESERVE:
            decision.issues.extend(decision.scheme.get('blockers') or [])
            _check_preserved(table_name, decision, schemes, selected_set, target_version_num)
        elif decision.action == FLATTEN:
            _check_flattened(decision)


def _check_preserved(table_name, decision, schemes, selected_set, target_version_num):
    methods = {decision.method} if decision.method else set()
    has_default = any(partition.is_default for partition in decision.partitions)
    ## whether a partition of this source is a relation of its own. See the contract in
    ## DatabaseConnector.fetch_table_partitioning(): PostgreSQL answers for its partitions as
    ## tables and every other engine keeps them as storage objects with no name of their own in
    ## the table list.
    partitions_are_tables = bool(decision.scheme.get('partitions_are_tables'))

    for partition in decision.partitions:
        child_scheme = schemes.get(partition.name) or {}
        if child_scheme.get('method'):
            methods.add(child_scheme['method'])
        if not partition.bound:
            decision.issues.append(
                f"the partition {partition.name} of {table_name} has no bound in the source "
                f"catalogue, so it cannot be created on the target. Read the scheme again, or "
                f"set source_partitioning: flatten for this table")
        if partition.name in selected_set or not partitions_are_tables:
            ## the warning below is about a table the FILTERS could have selected and did not,
            ## and that only exists where a partition is a relation of its own - which is
            ## PostgreSQL and nothing else. On every other source a partition is a storage
            ## object with no row in the table list, so it can never be "not selected", and
            ## saying so once per partition is one line per partition of noise: an Oracle
            ## INTERVAL table of 55 months produced 55 of them.
            continue
        decision.warnings.append(
            f"the partition {partition.name} of {table_name} is not selected by "
            f"include_tables / exclude_tables, and it is created anyway: the partitions of a "
            f"preserved scheme belong to their parent and are not selected one by one. Use "
            f"source_partitioning: flatten to migrate the table without them")

    if not decision.target_key_definition:
        decision.issues.append(
            f"{table_name} is partitioned on the source and its partitioning key could not be "
            f"read, so the same scheme cannot be built. Set source_partitioning: flatten for "
            f"this table to migrate it as one ordinary table")

    if not decision.partitions:
        decision.issues.append(
            f"{table_name} is partitioned on the source and not one of its partitions could be "
            f"read, so the target would be created partitioned with nothing under it - a table "
            f"which refuses EVERY row with 'no partition of relation ... found for row'. Set "
            f"source_partitioning: flatten for this table")

    ## a partition name PostgreSQL truncates is a name which collides with the one beside it,
    ## and the collision is found when the second CREATE TABLE fails. Oracle allows 128 bytes
    ## since 12.2 and PostgreSQL allows 63.
    for partition in decision.partitions:
        if len(str(partition.name).encode('utf-8')) > MAX_IDENTIFIER_LENGTH:
            decision.issues.append(
                f"the partition {partition.name} of {table_name} is "
                f"{len(str(partition.name).encode('utf-8'))} bytes long and PostgreSQL truncates "
                f"an identifier at {MAX_IDENTIFIER_LENGTH}, which turns two partitions into one "
                f"name. Rename it on the source, or set source_partitioning: flatten for this "
                f"table")

    if not target_version_num:
        decision.warnings.append(
            f"the version of the target could not be determined, so it was not checked that it "
            f"can build the scheme of {table_name}")
        return

    for method in sorted(methods):
        needed = METHOD_VERSIONS.get(method)
        if needed and target_version_num < needed:
            decision.issues.append(
                f"{table_name} is partitioned by {method} on the source, which needs PostgreSQL "
                f"{version_text(needed)} or newer - the target runs "
                f"{version_text(target_version_num)}. Upgrade the target, or set "
                f"source_partitioning: flatten for this table")
    if has_default and target_version_num < DEFAULT_PARTITION_VERSION:
        decision.issues.append(
            f"{table_name} has a DEFAULT partition, which needs PostgreSQL "
            f"{version_text(DEFAULT_PARTITION_VERSION)} or newer - the target runs "
            f"{version_text(target_version_num)}")

    if has_default:
        decision.warnings.append(
            f"{table_name} has a DEFAULT partition. It is carried over as it stands - and it is "
            f"worth knowing what it costs: attaching a new partition later makes PostgreSQL scan "
            f"the default partition to prove that no row in it belongs in the new one")


def _check_flattened(decision):
    scheme = decision.scheme
    if scheme.get('method') == 'HASH':
        decision.warnings.append(
            f"{decision.table_name} is partitioned by HASH on the source. Flattening it is the "
            f"one case where nothing is lost which could have been kept: a hash scheme prunes "
            f"only an equality on the key, and the rows of a hash partition have nothing in "
            f"common a query asks for")


## ------------------------------------------------------------------------------------
## The generator: a scheme the source never had - §5.3 of the design.
##
## The calendar is computed here, in Python, and not by asking the target to run
## generate_series() over values read from the source. That is what §0.3's quoting defect and
## its `'{max_value}0'::date` came out of, and a boundary is arithmetic which needs no database.
## ------------------------------------------------------------------------------------

DATE_RANGES = ('year', 'quarter', 'month', 'week', 'day')

## The name a generated partition is given, unless the entry writes its own. It is the name
## the migrator has always used, so a configuration which runs today keeps its partition names.
DEFAULT_PARTITION_NAME = '{table}_{range}_{start:%Y%m%d}'

def as_date(value):
    """
    The date of a value read from a partitioning column - a date, a timestamp, or the text
    either of them was answered as.
    """
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value
    text = str(value).strip()
    for length in (10, 19):
        try:
            return datetime.datetime.strptime(text[:length],
                                              '%Y-%m-%d' if length == 10 else '%Y-%m-%d %H:%M:%S').date()
        except ValueError:
            continue
    raise ValueError(f"{value!r} is not a date or a timestamp, so no range of partitions can be "
                     f"computed from it")


def range_start(value, date_range):
    """The start of the interval a date belongs to."""
    if date_range == 'year':
        return datetime.date(value.year, 1, 1)
    if date_range == 'quarter':
        return datetime.date(value.year, 3 * ((value.month - 1) // 3) + 1, 1)
    if date_range == 'month':
        return datetime.date(value.year, value.month, 1)
    if date_range == 'week':
        ## ISO weeks, which is what date_trunc('week', ...) of PostgreSQL uses: Monday
        return value - datetime.timedelta(days=value.weekday())
    if date_range == 'day':
        return value
    raise ValueError(f"unknown date_range '{date_range}' - one of {', '.join(DATE_RANGES)}")


def next_range_start(value, date_range):
    """The start of the interval behind this one - the EXCLUSIVE end of this one."""
    if date_range == 'year':
        return datetime.date(value.year + 1, 1, 1)
    if date_range == 'quarter':
        month = value.month + 3
        return datetime.date(value.year + (month > 12), month - 12 * (month > 12), 1)
    if date_range == 'month':
        month = value.month + 1
        return datetime.date(value.year + (month > 12), month - 12 * (month > 12), 1)
    if date_range == 'week':
        return value + datetime.timedelta(days=7)
    if date_range == 'day':
        return value + datetime.timedelta(days=1)
    raise ValueError(f"unknown date_range '{date_range}' - one of {', '.join(DATE_RANGES)}")


def range_partition_bounds(date_range, first_value, last_value):
    """
    The bounds of the partitions which cover first_value .. last_value.

    Three things this gets right which the generator it replaces did not:

      * the end of a partition is the START of the next one. PostgreSQL range bounds are
        `FROM (a) TO (b)` with a inclusive and b exclusive, and the old generator wrote
        `start + 1 interval - 1 day` - an inclusive end, so the last day of every month fell
        through the gap between one partition and the next and its rows fit nowhere;
      * `day` is one of the ranges. It was accepted by the configuration and produced no
        partitions at all - P3-3;
      * one whole interval of headroom at each end. A timestamptz value is compared in UTC and
        a bound written as a date is read in the TimeZone of the session, so a row within a few
        hours of the first or the last boundary can fall outside a range which covers exactly
        min..max. The headroom also gives the application a little room past the newest row -
        the `future:` window of §6 is still what really answers that.

    Returns [(start, end)] of `datetime.date`, in order, with no gap between them.
    """
    if date_range not in DATE_RANGES:
        raise ValueError(f"unknown date_range '{date_range}' - one of {', '.join(DATE_RANGES)}")
    first = as_date(first_value)
    last = as_date(last_value)
    if first is None or last is None:
        return []
    if last < first:
        first, last = last, first

    start = range_start(first, date_range)
    ## one interval before the oldest row, and one behind the newest
    start = range_start(start - datetime.timedelta(days=1), date_range)
    stop = next_range_start(range_start(last, date_range), date_range)
    stop = next_range_start(stop, date_range)

    bounds = []
    while start < stop:
        end = next_range_start(start, date_range)
        bounds.append((start, end))
        start = end
    return bounds


def generate_range_partitions(entry, table_name, first_value, last_value):
    """
    The partitions of one `target_partitioning` entry which asks for a `date_range`.

    Returns [Partition], with the bound written as PostgreSQL takes it. The name template of
    the entry - or the one this migrator has always used - decides what each is called, and a
    name which does not fit into an identifier, or which collides with another, is refused
    rather than silently truncated into its neighbour.
    """
    date_range = entry.get('date_range')
    bounds = range_partition_bounds(date_range, first_value, last_value)
    template = entry.get('partition_name') or DEFAULT_PARTITION_NAME
    partitions = []
    seen = {}
    for start, end in bounds:
        try:
            name = template.format(table=table_name, range=date_range, start=start, end=end)
        except (KeyError, IndexError, ValueError) as e:
            raise ValueError(
                f"partition_name '{template}' of {table_name} cannot be written: {e}. The names "
                f"it may use are {{table}}, {{range}}, {{start}} and {{end}}")
        if len(name) > MAX_IDENTIFIER_LENGTH:
            raise ValueError(
                f"the partition of {table_name} for {start} would be called '{name}', which is "
                f"{len(name)} characters - PostgreSQL truncates an identifier at "
                f"{MAX_IDENTIFIER_LENGTH} and two partitions would end up with one name. Write a "
                f"shorter partition_name")
        if name in seen:
            raise ValueError(
                f"partition_name '{template}' of {table_name} gives the partition for {start} "
                f"and the one for {seen[name]} the same name '{name}'")
        seen[name] = start
        partitions.append(Partition(
            name=name, parent=table_name,
            bound=f"FOR VALUES FROM ('{start}') TO ('{end}')", start=start, end=end))

    if entry.get('default_partition'):
        partitions.append(Partition(
            name=f'{table_name}_default', parent=table_name, bound='DEFAULT', is_default=True))
    return partitions


## The version a foreign key REFERENCING a partitioned table needs. A foreign key FROM one
## has been possible since 11.
REFERENCING_PARTITIONED_VERSION = 120000

## Where a partition count stops being a plan and starts being a cost of its own - §2.3 puts
## "several thousand" at the point where planning time shows on a query which cannot prune.
PARTITION_COUNT_WARNING = 500
PARTITION_COUNT_LIMIT = 5000


class Verdict:
    """What was found about one `target_partitioning` entry, and what may be built from it."""

    __slots__ = ('table_name', 'issues', 'warnings', 'notes', 'partitions', 'bounds_usable')

    def __init__(self, table_name):
        self.table_name = table_name
        ## cleared when the partitioning column cannot carry a calendar at all - the partitions
        ## are then not worked out, because "this is not a date" and "this value is not a date"
        ## are the same finding said twice
        self.bounds_usable = True
        ## blocking: the migration would fail later, so it is stopped now
        self.issues = []
        ## worth saying, and not a reason to stop
        self.warnings = []
        ## what was checked and found good, so that a passing entry says so as plainly as a
        ## failing one - a report which only speaks up when it is unhappy is a report nobody
        ## trusts when it is silent
        self.notes = []
        self.partitions = []

    @property
    def can_be_built(self):
        return not self.issues


def check_repartitioning(entry, columns, unique_keys, target_version_num=None,
                         table_exists=True, table_is_partition=False, facts=None,
                         first_value=None, last_value=None, existing_target_names=(),
                         bounds_were_read=False, bounds_can_be_read=True):
    """
    Whether one `target_partitioning` entry can be carried out - §4.4 of the design.

    entry               - the entry, as the configuration holds it
    columns             - the column names the source table really has
    unique_keys         - [{'name', 'columns', 'is_primary'}], or None where this connector
                          does not read them
    target_version_num  - the server version of the target
    table_exists        - whether the table the entry names is in the migration at all
    table_is_partition  - whether it is a partition of another table
    facts               - what fetch_partitioning_facts() answered, or None
    first_value,
    last_value          - the smallest and the largest value of the partitioning column, when
                          they were read; `bounds_were_read` says whether they were
    existing_target_names - the names the target schema already holds, for the collision check
    bounds_can_be_read  - whether this source CAN be asked for the values a column holds at all.
                          A DDL-only source cannot: there is no instance to ask, and an entry
                          which generates its partitions from a date_range is refused rather
                          than left to fail when they are worked out

    Returns a Verdict. Everything it answers is answerable before anything is created, and
    everything it refuses is a run which otherwise fails somewhere in the middle - most of them
    at the very end, after the data has been loaded.
    """
    verdict = Verdict(entry.get('table_name') or '<unnamed>')
    table_name = verdict.table_name

    if not table_exists:
        verdict.issues.append(
            f"target_partitioning names the table {table_name}, which the source schema does "
            f"not hold, or which include_tables / exclude_tables leaves out. Nothing would be "
            f"partitioned and nothing would say so")
        return verdict

    if table_is_partition:
        verdict.issues.append(
            f"target_partitioning names {table_name}, which is a PARTITION of another table on "
            f"the source. A partition is created with its parent and cannot be given a scheme "
            f"of its own here - name the parent instead")
        return verdict

    method = str(entry.get('partition_by') or '').upper()
    if method not in METHOD_VERSIONS:
        verdict.issues.append(
            f"target_partitioning for {table_name} asks for partition_by "
            f"'{entry.get('partition_by')}' - PostgreSQL has RANGE, LIST and HASH and nothing "
            f"else")
    elif target_version_num and target_version_num < METHOD_VERSIONS[method]:
        verdict.issues.append(
            f"target_partitioning for {table_name} asks for {method}, which needs PostgreSQL "
            f"{version_text(METHOD_VERSIONS[method])} or newer - the target runs "
            f"{version_text(target_version_num)}")

    partitioning_columns = partitioning_columns_of(entry)
    if not partitioning_columns:
        verdict.issues.append(f"target_partitioning for {table_name} names no partitioning column")
        return verdict

    known = {str(name).lower() for name in (columns or [])}
    missing = [name for name in partitioning_columns if name.lower() not in known]
    if known and missing:
        verdict.issues.append(
            f"target_partitioning for {table_name} names the column(s) {', '.join(missing)}, "
            f"which the table does not have. The entry is written in the names of the source")
        return verdict

    if entry.get('date_range') and (method != 'RANGE' or len(partitioning_columns) != 1):
        verdict.issues.append(
            f"target_partitioning for {table_name} has date_range, which belongs to a RANGE "
            f"over exactly one date or timestamp column - this entry is {method or 'unset'} "
            f"over {len(partitioning_columns)} column(s)")

    _check_the_table_itself(verdict, table_name, facts)
    _check_the_columns(verdict, entry, table_name, method, partitioning_columns, facts)
    _check_the_keys(verdict, table_name, partitioning_columns, unique_keys, facts)
    _check_the_rows_fit(verdict, entry, table_name, method, partitioning_columns, facts)
    _check_what_references_it(verdict, table_name, facts, target_version_num)
    _check_the_partitions(verdict, entry, table_name, method, partitioning_columns,
                          first_value, last_value, existing_target_names, bounds_were_read,
                          bounds_can_be_read)
    return verdict


def _check_the_table_itself(verdict, table_name, facts):
    """The shapes of table which cannot be partitioned at all, whatever the entry says."""
    if facts is None:
        return
    if facts.get('is_a_plain_inheritance_parent'):
        verdict.issues.append(
            f"{table_name} is the parent of a table INHERITANCE hierarchy on the source. "
            f"PostgreSQL cannot make a partitioned table out of a table which other tables "
            f"inherit from - the two are different mechanisms and a table is one or the other")
    if facts.get('inherits_from_a_plain_table'):
        verdict.issues.append(
            f"{table_name} INHERITS from another table on the source. A partitioned table "
            f"cannot inherit, so the entry and the hierarchy cannot both be built")
    for name in facts.get('exclusion_constraints') or []:
        verdict.issues.append(
            f"{table_name} carries the EXCLUSION constraint {name}. PostgreSQL does not allow "
            f"an exclusion constraint on a partitioned table unless every one of its columns is "
            f"compared with equality and the partitioning columns are among them - the table "
            f"would be created, the data would be loaded and the constraint would fail")
    estimate = facts.get('row_estimate')
    if estimate is not None and 0 <= estimate < 1000:
        verdict.warnings.append(
            f"{table_name} holds about {estimate} rows. Partitioning does not make a small "
            f"table faster - it prunes, it detaches cheaply and it maintains per partition, and "
            f"none of the three is worth much here")


def type_carries_a_calendar(type_name, date_range_types):
    """
    Whether a column type is one a `date_range` can be counted in.

    The precision is taken off before the comparison, because a source which writes one into
    the type name means the same type by it: Oracle's `TIMESTAMP(6) WITH TIME ZONE` and
    `TIMESTAMP(9) WITH TIME ZONE` are both the type PostgreSQL calls `timestamptz`, and a
    literal comparison against a list would refuse a column which carries a calendar perfectly
    well.
    """
    if not type_name:
        return False
    plain = re.sub(r'\s*\([^)]*\)', '', str(type_name)).strip().lower()
    return plain in {re.sub(r'\s*\([^)]*\)', '', str(candidate)).strip().lower()
                     for candidate in (date_range_types or ())}


def column_facts_of(facts, name):
    """
    What is known about one column, found however the source spells its name.

    The entry is written by hand and the catalogue answers in the case the engine keeps -
    Oracle upper, PostgreSQL lower - and the two need not match. A lookup which is not
    case-insensitive answers None for a column which is there, and every check about it is
    then skipped in silence.
    """
    columns = (facts or {}).get('columns') or {}
    if name in columns:
        return columns[name]
    wanted = str(name).lower()
    for column_name, column in columns.items():
        if str(column_name).lower() == wanted:
            return column
    return None


def _check_the_columns(verdict, entry, table_name, method, partitioning_columns, facts):
    """The partitioning columns themselves: their type, and what PostgreSQL will do with them."""
    if facts is None:
        verdict.warnings.append(
            f"the columns of {table_name} could not be read from this source, so it was NOT "
            f"checked that their types can carry a {method or 'partitioning'} key")
        return
    date_range = entry.get('date_range')
    date_types = facts.get('date_range_types') or ()
    for name in partitioning_columns:
        column = column_facts_of(facts, name)
        if column is None:
            continue
        if column.get('is_generated'):
            verdict.issues.append(
                f"{table_name}.{name} is a GENERATED column. PostgreSQL refuses a generated "
                f"column in a partition key")
        if method in ('RANGE', 'LIST') and not column.get('has_btree_opclass'):
            verdict.issues.append(
                f"{table_name}.{name} is {column.get('type_name')}, which has no default btree "
                f"operator class - a {method} partition key needs one, because the bounds are "
                f"compared with < and =")
        if method == 'HASH' and not column.get('has_hash_opclass'):
            verdict.issues.append(
                f"{table_name}.{name} is {column.get('type_name')}, which has no default hash "
                f"operator class - a HASH partition key needs one")
        if date_range and not type_carries_a_calendar(column.get('type_name'), date_types):
            verdict.bounds_usable = False
            verdict.issues.append(
                f"target_partitioning for {table_name} asks for date_range: {date_range} over "
                f"{name}, which is {column.get('type_name')}. A range of dates can only be "
                f"counted over {' or '.join(date_types)} - write the partitions out, or "
                f"partition by a column which carries a date")
        else:
            verdict.notes.append(f"{name} is {column.get('type_name')}")


## What to write instead, when a key does not contain the partitioning columns. The finding is
## the same either way - §3.1 is a property of PostgreSQL - and what the user can do about it is
## not: an entry they wrote can be taken out, and a scheme which came off the source cannot.
REMEDY_FOR_AN_ENTRY = 'or do not partition this table by {columns}'
REMEDY_FOR_A_PRESERVED_SCHEME = (
    'or set source_partitioning: flatten for this table, which migrates it as one ordinary '
    'table and keeps the key as it is')


def unique_key_findings(table_name, partitioning_columns, keys, remedy=REMEDY_FOR_AN_ENTRY):
    """
    §3.1, key by key: the rule which breaks migrations.

    Every unique constraint and every unique index of a partitioned table must contain all of
    its partitioning columns, and this is where a migration fails at the very end - the table is
    created, the data is loaded, and the constraint is refused.

    It is asked of a `target_partitioning` entry and of a scheme carried over from the source
    alike. The second is not the smaller case: Oracle keeps a primary key which does not contain
    the partitioning column in a GLOBAL index, which is legal there, ordinary there, and has no
    counterpart here at all.

    Returns (issues, notes).
    """
    issues = []
    notes = []
    if not keys:
        return issues, ['no primary key and no unique constraint - nothing to extend']
    for key in keys:
        key_columns = {str(name).lower() for name in (key.get('columns') or [])}
        if not key_columns:
            continue
        kind = 'PRIMARY KEY' if key.get('is_primary') else 'UNIQUE'
        written = ', '.join(key.get('columns') or [])
        absent = [name for name in partitioning_columns if name.lower() not in key_columns]
        if not absent:
            notes.append(f"{kind} {key.get('name')} ({written}) contains "
                         f"{', '.join(partitioning_columns)}")
            continue
        issues.append(
            f"{kind} {key.get('name')} of {table_name} is ({written}) and does not contain "
            f"{', '.join(absent)}. PostgreSQL refuses a unique constraint on a partitioned table "
            f"which does not contain every partitioning column, so the table would be created, "
            f"the data would be loaded and the constraint would fail. Add "
            f"{', '.join(absent)} to the key, "
            + remedy.format(columns=', '.join(partitioning_columns)))
    return issues, notes


def check_preserved_keys(decision, unique_keys):
    """
    §3.1 for a table whose scheme is carried over from the source as it stands.

    Oracle is why this is not the same check as the one a `target_partitioning` entry gets: a
    primary key which does not contain the partitioning column is legal on Oracle - it lives in
    a GLOBAL index - and PostgreSQL has no global index. So a scheme which nobody chose and
    which the source has run for years is refused here, before the table is created, rather
    than at the end of the run when the constraint is added to a table already holding the data.
    """
    columns = decision.scheme.get('columns') or []
    if decision.action != PRESERVE or not columns:
        return
    if unique_keys is None:
        decision.warnings.append(
            f"the unique keys of {decision.table_name} could not be read from this source, so it "
            f"was NOT checked that they contain the partitioning columns. PostgreSQL refuses a "
            f"primary key or a unique constraint on a partitioned table which does not")
        return
    issues, _notes = unique_key_findings(decision.table_name, columns, unique_keys,
                                         remedy=REMEDY_FOR_A_PRESERVED_SCHEME)
    decision.issues.extend(issues)


def _check_the_keys(verdict, table_name, partitioning_columns, unique_keys, facts):
    """§3.1: the rule which breaks migrations."""
    keys = unique_keys
    if keys is None and facts is not None:
        keys = facts.get('unique_keys')
    if keys is None:
        verdict.warnings.append(
            f"the unique keys of {table_name} could not be read from this source, so it was NOT "
            f"checked that they contain the partitioning columns. PostgreSQL refuses a primary "
            f"key or a unique constraint on a partitioned table which does not")
        return
    issues, notes = unique_key_findings(table_name, partitioning_columns, keys)
    verdict.issues.extend(issues)
    verdict.notes.extend(notes)


def _check_the_rows_fit(verdict, entry, table_name, method, partitioning_columns, facts):
    """
    A row which fits no partition is refused, one row at a time, in the middle of the data
    migration. The one shape of that which is answerable from the catalogue is the NULL.
    """
    if facts is None or method != 'RANGE':
        return
    has_default = bool(entry.get('default_partition'))
    for name in partitioning_columns:
        column = column_facts_of(facts, name)
        if column is None:
            continue
        if column.get('not_null'):
            verdict.notes.append(f"{name} is NOT NULL - every row has a partition to go to")
            continue
        if has_default:
            verdict.notes.append(
                f"{name} is nullable, and default_partition is set - a NULL has somewhere to go")
            continue
        null_fraction = column.get('null_fraction')
        if null_fraction is None:
            verdict.warnings.append(
                f"{table_name}.{name} is nullable and nobody has ANALYZEd the table, so it is "
                f"NOT known whether it holds a NULL. A NULL fits no RANGE partition except the "
                f"DEFAULT one, and the rows holding it would be refused one at a time in the "
                f"middle of the data migration - set default_partition: true")
        elif null_fraction > 0:
            verdict.issues.append(
                f"{table_name}.{name} is nullable and the statistics of the source say about "
                f"{null_fraction * 100:.1f}% of its rows are NULL. A NULL fits no RANGE "
                f"partition except the DEFAULT one, and this entry has none - those rows cannot "
                f"be loaded. Set default_partition: true, or partition by a column which is "
                f"NOT NULL")
        else:
            verdict.notes.append(
                f"{name} is nullable and the statistics of the source hold no NULL in it")


def _check_what_references_it(verdict, table_name, facts, target_version_num):
    """A foreign key pointing AT a partitioned table needs PostgreSQL 12."""
    if facts is None:
        return
    referencing = facts.get('referenced_by') or []
    if not referencing:
        return
    named = ', '.join(f"{item['table']}.{item['name']}" for item in referencing[:4])
    if target_version_num and target_version_num < REFERENCING_PARTITIONED_VERSION:
        verdict.issues.append(
            f"{len(referencing)} foreign key(s) reference {table_name} ({named}). A foreign key "
            f"referencing a PARTITIONED table needs PostgreSQL "
            f"{version_text(REFERENCING_PARTITIONED_VERSION)} or newer - the target runs "
            f"{version_text(target_version_num)}")
    else:
        verdict.notes.append(
            f"{len(referencing)} foreign key(s) reference it ({named}) - allowed on a "
            f"partitioned table from PostgreSQL "
            f"{version_text(REFERENCING_PARTITIONED_VERSION)} on")


def _check_the_partitions(verdict, entry, table_name, method, partitioning_columns,
                          first_value, last_value, existing_target_names, bounds_were_read,
                          bounds_can_be_read=True):
    """
    The partitions the entry would really produce: how many, what they are called, and whether
    there would be any at all.
    """
    date_range = entry.get('date_range')
    if method == 'RANGE' and not date_range and not entry.get('partitions'):
        verdict.issues.append(
            f"target_partitioning for {table_name} asks for RANGE and says nothing about which "
            f"partitions to create. The table would be created partitioned and EMPTY, and every "
            f"row of the migration would be refused with 'no partition of relation "
            f"{table_name} found for row' - write a date_range")
        return
    if method == 'HASH':
        verdict.issues.append(
            f"target_partitioning for {table_name} asks for HASH, and the number of partitions "
            f"to create it with is not part of the configuration language yet. The table would "
            f"be created partitioned and EMPTY, and every row would be refused")
        return
    if method == 'LIST':
        verdict.issues.append(
            f"target_partitioning for {table_name} asks for LIST, and the values of each "
            f"partition are not part of the configuration language yet - only this migration's "
            f"user knows them. The table would be created partitioned and EMPTY, and every row "
            f"would be refused")
        return
    if not date_range:
        return
    if not verdict.bounds_usable:
        ## the column cannot carry a calendar, which has already been said - working the
        ## partitions out would only say it again in the words of the generator
        return

    if not bounds_can_be_read:
        ## a source with no instance behind it - the two DDL-only Db2 connectors. The calendar
        ## of a date_range is worked out from the values the column really holds, and there is
        ## nothing to ask. Blocking, because what it produces otherwise is a partitioned table
        ## with no partitions under it, which refuses every row of the migration
        verdict.issues.append(
            f"target_partitioning for {table_name} generates its partitions from a date_range, "
            f"which needs the smallest and the largest value of {partitioning_columns[0]} - and "
            f"this source has no database to ask: its structure comes from DDL files and its "
            f"data from CSV files. The table would be created partitioned with nothing under it "
            f"and every row would be refused. Take the entry out and partition the table after "
            f"the migration, or migrate from an instance")
        return
    if not bounds_were_read:
        ## BLOCKING, and it stood here as a warning until 2026-08-27. The outcome is the same
        ## one the three branches above call blocking - a partitioned table with nothing under
        ## it - and only the reason differs: there the source has no instance to ask, here the
        ## question was asked and failed. An Informix run took this path, created
        ## `currency_rates` partitioned with ZERO partitions, refused all 442 rows with "no
        ## partition of relation currency_rates found for row", and finished saying
        ## "Migration Done". A warning is not enough for an outcome that loses the table.
        verdict.issues.append(
            f"target_partitioning for {table_name} generates its partitions from a date_range, "
            f"which needs the smallest and the largest value of {partitioning_columns[0]} - and "
            f"the source refused the question. The table would be created partitioned with "
            f"nothing under it and every row of it would be refused with 'no partition of "
            f"relation {table_name} found for row'. The reason the source gave is logged above "
            f"this line by read_partitioning_bounds")
        return
    if first_value is None or last_value is None:
        verdict.warnings.append(
            f"{table_name} holds no row in {partitioning_columns[0]}, so no partition can be "
            f"generated from its values. The table is created partitioned and empty - which is "
            f"right for a table with no rows, and every INSERT after the migration is refused "
            f"until a partition exists")
        return

    try:
        partitions = generate_range_partitions(entry, table_name, first_value, last_value)
    except ValueError as e:
        verdict.issues.append(f"target_partitioning for {table_name}: {e}")
        return

    verdict.partitions = partitions
    count = len(partitions)
    ranged = [partition for partition in partitions if partition.start is not None]
    if ranged:
        verdict.notes.append(
            f"{count} partition(s) by {date_range}, {ranged[0].start} .. {ranged[-1].end}"
            + (' plus a DEFAULT partition' if partitions[-1].is_default else ''))

    if count > PARTITION_COUNT_LIMIT:
        verdict.issues.append(
            f"target_partitioning for {table_name} by {date_range} would create {count} "
            f"partitions. Every one of them is a table with its own statistics, its own indexes "
            f"and its own place in every plan which cannot prune - this is past what a scheme "
            f"can carry. Use a coarser date_range")
    elif count > PARTITION_COUNT_WARNING:
        verdict.warnings.append(
            f"target_partitioning for {table_name} by {date_range} creates {count} partitions. "
            f"§2.3 of the design puts several thousand at the point where planning time starts "
            f"to show on a query which cannot prune - a coarser date_range may be the better "
            f"scheme")

    existing = {str(name).lower() for name in (existing_target_names or [])}
    colliding = [partition.name for partition in partitions if partition.name.lower() in existing]
    if colliding:
        verdict.issues.append(
            f"the partition(s) {', '.join(colliding[:5])} of {table_name} would be created under "
            f"a name the target schema already holds. Write a partition_name which does not "
            f"collide, or take the object out of the target schema first")


def partitioning_columns_of(entry):
    """The partitioning columns of one target_partitioning entry, as a list of bare names."""
    raw = entry.get('partitioning_columns')
    if not raw:
        return []
    if isinstance(raw, (list, tuple)):
        parts = [str(name) for name in raw]
    else:
        parts = str(raw).split(',')
    return [part.strip().strip('"') for part in parts if part.strip().strip('"')]
