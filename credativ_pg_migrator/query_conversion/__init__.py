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
Conversion of the SELECT statements an application holds as text, from the dialect of the
source database into the one of the migrated PostgreSQL database.

The step runs after a migration, over the migrated target - it creates nothing and moves no
data. It reads files of statements, converts every SELECT with the same connector code which
converts the views, tests the result against the target and writes files a developer reads,
with the outcome of each test in the file.

The design is described in development/APPLICATION_QUERIES_CONVERSION_STRATEGY.md.
"""

from credativ_pg_migrator.query_conversion.workflow import QueryConverter

__all__ = ['QueryConverter']
