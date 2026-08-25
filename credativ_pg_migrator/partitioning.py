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


def version_text(version_num):
    """A PostgreSQL server version number as the number a person reads."""
    if not version_num:
        return 'unknown'
    return str(version_num // 10000)


class Partition:
    """One partition which has to be created, and everything needed to create it."""

    __slots__ = ('name', 'parent', 'bound', 'key_definition', 'level', 'is_default')

    def __init__(self, name, parent, bound, key_definition='', level=2, is_default=False):
        self.name = name
        self.parent = parent
        self.bound = bound
        ## set when the partition is itself partitioned - the scheme has more than one level
        self.key_definition = key_definition
        self.level = level
        self.is_default = is_default

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
        return self.scheme.get('key_definition', '')

    @property
    def method(self):
        return self.scheme.get('method', '')

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
            bound=partition.get('bound', ''),
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
        if decision.action == PRESERVE:
            _check_preserved(table_name, decision, schemes, selected_set, target_version_num)
        elif decision.action == FLATTEN:
            _check_flattened(decision)


def _check_preserved(table_name, decision, schemes, selected_set, target_version_num):
    methods = {decision.method} if decision.method else set()
    has_default = any(partition.is_default for partition in decision.partitions)

    for partition in decision.partitions:
        child_scheme = schemes.get(partition.name) or {}
        if child_scheme.get('method'):
            methods.add(child_scheme['method'])
        if not partition.bound:
            decision.issues.append(
                f"the partition {partition.name} of {table_name} has no bound in the source "
                f"catalogue, so it cannot be created on the target. Read the scheme again, or "
                f"set source_partitioning: flatten for this table")
        if partition.name in selected_set:
            continue
        decision.warnings.append(
            f"the partition {partition.name} of {table_name} is not selected by "
            f"include_tables / exclude_tables, and it is created anyway: the partitions of a "
            f"preserved scheme belong to their parent and are not selected one by one. Use "
            f"source_partitioning: flatten to migrate the table without them")

    if not decision.key_definition:
        decision.issues.append(
            f"{table_name} is partitioned on the source and its partitioning key could not be "
            f"read, so the same scheme cannot be built. Set source_partitioning: flatten for "
            f"this table to migrate it as one ordinary table")

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


def check_repartitioning(entry, columns, unique_keys, target_version_num=None,
                         table_exists=True, table_is_partition=False):
    """
    Whether one `target_partitioning` entry can be carried out - §4.4 of the design.

    entry              - the entry, as the configuration holds it
    columns            - the column names the source table really has
    unique_keys        - [{'name': str, 'columns': [str], 'is_primary': bool}] of the table,
                         or None when this connector does not read them
    target_version_num - the server version of the target
    table_exists       - whether the table named by the entry is in the source at all
    table_is_partition - whether it is a partition of another table

    Returns (issues, warnings). Everything it answers is answerable before anything is created.
    """
    issues = []
    warnings = []
    table_name = entry.get('table_name') or '<unnamed>'

    if not table_exists:
        issues.append(
            f"target_partitioning names the table {table_name}, which the source schema does "
            f"not hold, or which include_tables / exclude_tables leaves out. Nothing would be "
            f"partitioned and nothing would say so")
        return issues, warnings

    if table_is_partition:
        issues.append(
            f"target_partitioning names {table_name}, which is a PARTITION of another table on "
            f"the source. A partition is created with its parent and cannot be given a scheme "
            f"of its own here - name the parent instead")
        return issues, warnings

    method = str(entry.get('partition_by') or '').upper()
    if method not in METHOD_VERSIONS:
        issues.append(
            f"target_partitioning for {table_name} asks for partition_by '{entry.get('partition_by')}' "
            f"- PostgreSQL has RANGE, LIST and HASH and nothing else")
    elif target_version_num and target_version_num < METHOD_VERSIONS[method]:
        issues.append(
            f"target_partitioning for {table_name} asks for {method}, which needs PostgreSQL "
            f"{version_text(METHOD_VERSIONS[method])} or newer - the target runs "
            f"{version_text(target_version_num)}")

    partitioning_columns = partitioning_columns_of(entry)
    if not partitioning_columns:
        issues.append(f"target_partitioning for {table_name} names no partitioning column")
        return issues, warnings

    known = {str(name).lower() for name in (columns or [])}
    missing = [name for name in partitioning_columns if name.lower() not in known]
    if known and missing:
        issues.append(
            f"target_partitioning for {table_name} names the column(s) {', '.join(missing)}, "
            f"which the table does not have. The entry is written in the names of the source")

    if entry.get('date_range') and (method != 'RANGE' or len(partitioning_columns) != 1):
        issues.append(
            f"target_partitioning for {table_name} has date_range, which belongs to a RANGE "
            f"over exactly one date or timestamp column - this entry is {method or 'unset'} "
            f"over {len(partitioning_columns)} column(s)")

    if unique_keys is None:
        warnings.append(
            f"the unique keys of {table_name} could not be read from this source, so it was NOT "
            f"checked that they contain the partitioning columns. PostgreSQL refuses a primary "
            f"key or a unique constraint on a partitioned table which does not")
        return issues, warnings

    for key in unique_keys:
        key_columns = {str(name).lower() for name in (key.get('columns') or [])}
        if not key_columns:
            continue
        absent = [name for name in partitioning_columns if name.lower() not in key_columns]
        if not absent:
            continue
        kind = 'PRIMARY KEY' if key.get('is_primary') else 'UNIQUE'
        issues.append(
            f"{kind} {key.get('name')} of {table_name} is ({', '.join(key.get('columns') or [])}) "
            f"and does not contain {', '.join(absent)}. PostgreSQL refuses a unique constraint "
            f"on a partitioned table which does not contain every partitioning column, so the "
            f"table would be created, the data would be loaded and the constraint would fail. "
            f"Add {', '.join(absent)} to the key, or do not partition this table by "
            f"{', '.join(partitioning_columns)}")
    return issues, warnings


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

## PostgreSQL refuses an identifier longer than this, and truncates the rest of it silently -
## which turns two partitions of a long table into one name and a collision.
MAX_IDENTIFIER_LENGTH = 63


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
            bound=f"FOR VALUES FROM ('{start}') TO ('{end}')"))

    if entry.get('default_partition'):
        partitions.append(Partition(
            name=f'{table_name}_default', parent=table_name, bound='DEFAULT', is_default=True))
    return partitions


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
