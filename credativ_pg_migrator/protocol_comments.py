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
Descriptions of the tables the migrator keeps in its own database - the protocol tables,
which record what a migration planned and what it really did, and the ddl_* tables, which
hold the DDL of a source parsed out of script files when the source is migrated from its DDL
export instead of from a live connection.

The texts are written into the database itself as COMMENT ON TABLE / COMMENT ON COLUMN
(MigratorTables.apply_comments), so everything reading the catalog gets them - the web GUI
shows them as the hints of a table and of its columns, psql shows the same texts with \\dt+
and \\d+. They say what a value means and how it is to be read - an empty task_completed, a
false success, an id which is not a reference - and not only what the column is called.

COMMON_COLUMN_COMMENTS holds the columns which repeat in nearly every protocol table; a
table which uses one of them with a different meaning overrides it in its own entry.
"""


COMMON_COLUMN_COMMENTS = {
    'id': 'Primary key of the record. It is the value the tasks table refers to in its object_protocol_id column and the value the log messages of the migration name.',
    'created_at': 'When the record was written.',
    'inserted': 'When the record was written.',
    'inserted_at': 'When the record was written.',
    'task_created': 'When the planner wrote this task down. The whole plan is written before the first task of it is carried out, so this is the time of the planning phase and not of the work.',
    'task_started': 'When a worker picked the task up. Empty = the task was never started; filled while task_completed stays empty = the task is still running, or the run was interrupted in the middle of it.',
    'task_completed': 'When the task ended, successfully or not.',
    'success': 'Outcome of the task: true = done, false = it failed and the reason stands in message, empty = it has not finished yet.',
    'message': 'What happened - the error of a failed task, or the note of a task which was carried out in a different way than the source asked for. A filled message on a successful task is a warning worth reading.',
    'worker_id': 'The parallel worker which processed the row. One table is always processed by one worker, so this also tells which rows were written at the same time.',
    'source_schema_name': 'Schema of the object in the source database. Which part of the name of the source this is depends on the engine - the owner in Sybase ASE and Informix, the library or the database in DB2, the user in Oracle.',
    'source_table_name': 'Name of the table in the source database, spelled the way the source spells it.',
    'source_table_id': 'The identifier the source database itself gave the table, for the engines which have one (Sybase ASE, Informix, DB2) - empty for the others. It is a value of the source catalog and not a reference to a row of the protocol schema.',
    'target_schema_name': 'Schema of the target PostgreSQL database the object is created in.',
    'target_table_name': 'Name of the table in the target PostgreSQL database - the name the table of the source was mapped to.',
    'target_alias_name': 'The second name the table is also known under in the target, created as a view over it, when the source knows the table under more than one name. Empty when there is none.',
    'final_valid': "Result of the check run at the end of the migration, which asks the catalogue of the target whether the object is THERE: true = it is, false = there is DDL for it and it is not there (see final_valid_message), empty = nothing was ever created for it, so there was nothing to look for. Being there is NOT the same as doing what the object of the source did, and no catalogue can say that: a view which is there has had its query resolved by PostgreSQL, and a PL/pgSQL routine which is there has had the syntax of its body parsed and nothing more - a body which reads a table that is not there is created without complaint and fails at the first call.",
    'final_valid_message': 'What the closing check established for this object, and what it did not - it says which of the two it means, per kind of object.',
    'validated_at': 'When the validation wrote this result.',
    'passed': 'Whether the source and the target agree on everything this row compares.',
}


def build_comments_catalog(config_parser):
    """
    Returns {table_name: {'comment': str, 'columns': {column_name: str}}} for every table the
    migrator creates in its schema. The names of the protocol tables carry the configured
    prefix, so the catalog is built with the same getters which create the tables.
    A column which is not named here falls back to COMMON_COLUMN_COMMENTS.
    """
    protocol = config_parser.get_protocol_name()
    catalog = {}

    catalog[protocol] = {
        'comment': (
            'The journal of the migration - one row for every object the migrator planned and for every action it '
            'carried out on it, in the order things happened. It is the table which ties the schema together: '
            'object_type says which kind of object a row is about and object_protocol_id points at the row of the '
            f'detail table of that kind (a table row at "{protocol}_tables", an index row at "{protocol}_indexes" '
            'and so on), so a run can be followed from here into the tables holding the DDL, the counts and the errors.'
        ),
        'columns': {
            'object_type': 'The kind of object the row is about: table, index, constraint, view, trigger, funcproc, sequence, alias, column, domain, collation, text_search, user_defined_type, data_migration, data_chunk, data_source, target_column_alteration, source/target_table_partitioning, or main for a phase of the run itself.',
            'object_name': 'Name of the object the row is about. For a row of the type main it is the phase and the step of the run instead.',
            'object_action': "What was done with the object: 'create' - it was created in the target; 'alter' - an existing object of the target was changed; 'start' - a phase of the run began; 'pk_range' - a range of the primary key was handed to a worker.",
            'object_ddl': 'The statement which was sent to the target - the SQL a GUI shows when the row is opened. Empty for a row which carries no statement of its own.',
            'insertion_timestamp': 'When the row was written, which is when the task was planned - not when it was carried out.',
            'execution_timestamp': 'When the result of the statement was reported back. Empty = the task was not carried out yet.',
            'execution_success': 'Whether the target accepted the statement: true = yes, false = it was refused and the answer stands in execution_error_message, empty = it was not carried out yet.',
            'execution_error_message': 'The error the target answered with. Empty for a statement which went through.',
            'row_type': "What the row is - 'info' for the ordinary course of the migration. Any other value marks a row written to draw attention to something.",
            'execution_results': 'What the execution returned beyond success or failure - the number of rows moved, for instance.',
            'object_protocol_id': 'Id of the row in the detail table which belongs to object_type - e.g. the id of the row in the tables protocol table when object_type is table. Together with object_type it addresses exactly one row there.',
        },
    }

    catalog[config_parser.get_protocol_name_main()] = {
        'comment': (
            'The phases of the migration run and how long each of them took - the top level view of a run. One row per '
            'task and subtask, in the order they were started. Reading it from top to bottom tells where a run stands '
            'and which step of it was expensive.'
        ),
        'columns': {
            'task_name': 'The phase of the migration - the preparation of the tables, the migration of the data, the creation of the indexes and so on.',
            'subtask_name': 'The step inside the phase.',
            'task_started': 'When the phase or the step began.',
            'task_completed': 'When it ended. Empty while it is still running.',
            'success': 'Outcome of the phase: true = done, false = it failed and the reason stands in message, empty = it is still running.',
        },
    }

    catalog[config_parser.get_protocol_name_user_defined_types()] = {
        'comment': (
            'The user defined types of the source and what became of them in the target. A type PostgreSQL can hold as '
            'a type of its own is created there; for the rest the type is resolved to the type behind it, and '
            'target_basic_type is what every column declared with it receives.'
        ),
        'columns': {
            'source_type_name': 'Name of the type in the source database.',
            'source_type_sql': 'The declaration of the type as the source database states it.',
            'target_type_name': 'Name the type was given in the target, when a type of its own was created for it.',
            'target_type_sql': 'The statement sent to the target. Empty when no type of its own was created.',
            'target_basic_type': 'The PostgreSQL type the type of the source resolves to. A column declared with the type of the source is created with this type when the type itself was not migrated.',
            'type_comment': 'Description of the type read from the source catalog and carried over to the target.',
        },
    }

    catalog[config_parser.get_protocol_name_domains()] = {
        'comment': (
            'The domains of the source and how their rule was expressed in the target. A domain PostgreSQL can hold is '
            'created as a domain; a rule which a domain cannot carry is attached to every column using it instead, '
            'and migrated_as says which of the two happened.'
        ),
        'columns': {
            'source_domain_name': 'Name of the domain in the source database.',
            'source_domain_sql': 'The declaration of the domain as the source database states it.',
            'source_domain_check_sql': 'The rule the source attaches to the domain - its CHECK clause, or the construct of the source engine which acts as one.',
            'target_domain_name': 'Name of the domain in the target, when a domain was created.',
            'target_domain_sql': 'The statement sent to the target.',
            'migrated_as': "How the domain was expressed in the target: 'DOMAIN' - a domain of PostgreSQL was created; 'CHECK CONSTRAINT' - the rule was written into every table which uses the domain, because a domain of PostgreSQL cannot carry it.",
            'domain_comment': 'Description of the domain read from the source catalog and carried over to the target.',
        },
    }

    catalog[config_parser.get_protocol_name_collations()] = {
        'comment': (
            'The collations of the source and the collations created for them in the target. A collation decides the '
            'sort order and the comparison of text, so a column migrated without its collation sorts differently than '
            'it did in the source.'
        ),
        'columns': {
            'source_collation_name': 'Name of the collation in the source database.',
            'source_collation_sql': 'The declaration of the collation as the source database states it.',
            'target_collation_name': 'Name of the collation in the target.',
            'target_collation_sql': 'The statement sent to the target.',
            'collation_provider': "Which provider the collation of the target uses - 'icu' or 'libc'. It decides how closely the sort order of the source is reproduced.",
            'collation_comment': 'Description of the collation read from the source catalog and carried over to the target.',
        },
    }

    catalog[config_parser.get_protocol_name_text_search()] = {
        'comment': (
            'The full text search objects of the source - configurations, dictionaries, parsers and templates - and '
            'their counterparts in the target. They belong together: a configuration is worthless without the '
            'dictionaries it names, so the whole set is migrated and recorded here.'
        ),
        'columns': {
            'source_object_name': 'Name of the object in the source database.',
            'source_object_sql': 'The declaration of the object as the source database states it.',
            'target_object_name': 'Name of the object in the target.',
            'target_object_sql': 'The statement sent to the target.',
            'object_type': 'Which part of the text search machinery the row is about - a search configuration, a dictionary, a parser or a template.',
            'object_comment': 'Description of the object read from the source catalog and carried over to the target.',
        },
    }

    catalog[config_parser.get_protocol_name_default_values()] = {
        'comment': (
            'The named default values of the source. Some engines (Sybase ASE, SQL Anywhere) keep a default as an '
            'object of its own and bind it to the columns which use it, while PostgreSQL writes the default into the '
            'column - so the value is extracted here and put into every column bound to the object.'
        ),
        'columns': {
            'default_value_schema': 'Schema the default object belongs to in the source database.',
            'default_value_name': 'Name of the default object in the source database. It is the name the columns of the source refer to.',
            'target_default_value_name': 'The name the object really has in the target - the spelling names_case_handling produced. The column next to it holds the spelling of the source, which is kept unchanged: two objects of the source which differ only in the case of their letters are two different objects, and the record of what was read has to say so.',
            'default_value_sql': 'The declaration of the default object as the source database states it.',
            'extracted_default_value': 'The value itself, taken out of the declaration and written into the DEFAULT clause of every column which was bound to the object.',
            'default_value_data_type': 'The type of the extracted value, which decides how it is written into the column.',
            'default_value_comment': 'Description of the default object read from the source catalog.',
        },
    }

    catalog[config_parser.get_protocol_name_target_columns_alterations()] = {
        'comment': (
            'The columns of the target whose data type had to be changed after the tables were created. The migrator '
            'alters a column when the target refuses what the mapping produced - most often the two sides of a '
            'foreign key, which PostgreSQL only accepts when they have the same type. Every row here is a place where '
            'the target does not have the type the mapping first chose.'
        ),
        'columns': {
            'target_column': 'The column which was altered.',
            'reason': 'Why the column had to be altered - which requirement of the target the original type did not meet.',
            'original_data_type': 'The type the column was created with.',
            'altered_data_type': 'The type it was changed to.',
        },
    }

    catalog[config_parser.get_protocol_name_new_objects()] = {
        'comment': (
            'Objects which do not come from the source database at all but are asked for in the configuration of the '
            'migration, and are created in the target together with everything else.'
        ),
        'columns': {
            'object_comment': 'What the object is for, as it is written in the configuration.',
            'object_type': 'The kind of object which is created.',
            'object_sql': 'The statement which creates it in the target, taken from the configuration.',
        },
    }

    catalog[config_parser.get_protocol_name_tables()] = {
        'comment': (
            'One row per table of the migration - the table of the source, the table created for it in the target, the '
            'DDL of both, the columns of both and the row counts. It is the central table of a migration: comparing '
            'source_table_rows_limited with target_table_rows says whether the data really arrived, and '
            'target_table_sql is the statement the target was given.'
        ),
        'columns': {
            'source_columns': 'The columns of the source table as they were read from its catalog, as JSON - the full description of every column, in the order of the source.',
            'source_table_rows_all': 'How many rows the source table holds.',
            'source_table_rows_limited': 'How many rows are to be migrated. It is smaller than source_table_rows_all when the configuration limits the table with a WHERE clause, and the same value when it does not - it is the number the migrated table is to be compared against.',
            'source_table_description': 'Description of the table read from the source catalog.',
            'source_table_sql': 'The CREATE statement of the table as the source database states it, for the engines which can give it.',
            'target_columns': 'The columns created in the target, as JSON - the counterpart of source_columns after the type mapping was applied.',
            'target_table_rows': 'How many rows the target table really holds after the data was migrated.',
            'target_table_sql': 'The CREATE TABLE statement which was sent to the target.',
            'table_comment': 'Comment of the table, carried over from the source to the target.',
            'create_partitions_sql': 'The statements which create the partitions of the target table. Empty for a table which is not partitioned.',
            'partitioned': 'Whether the target table is created partitioned - because the source partitions it and migration.source_partitioning keeps the scheme, or because target_partitioning asks for one.',
            'partitioned_by': 'The partitioning method of the target table: RANGE, LIST or HASH.',
            'partitioning_columns': 'The columns the target table is partitioned by.',
        },
    }

    catalog[config_parser.get_protocol_name_source_table_partitioning()] = {
        'comment': (
            'How a partitioned table is partitioned in the source database - one row per level of the partitioning. It '
            'is what the partitions of the target are built from.'
        ),
        'columns': {
            'source_table_partitioning_level': 'The level of the partitioning this row describes - 1 for the partitioning of the table, 2 for the partitioning of its partitions, and so on.',
            'source_partitioning_method': 'How the source partitions the table on this level - RANGE, LIST, HASH, or what the engine calls it.',
            'source_root_table_name': 'The table at the top of the partitioning tree this level belongs to. A scheme of more than one level is recorded one row per level, and this is what says which rows belong together.',
            'source_partition_columns': 'The columns the source partitions the table by on this level.',
            'source_partition_ranges': 'The bounds of the partitions of the source on this level.',
        },
    }

    catalog[config_parser.get_protocol_name_target_table_partitioning()] = {
        'comment': (
            'How the partitioned table was really partitioned in the target - one row per level. It is the counterpart '
            'of the source table partitioning table and says what PostgreSQL received, which is not always what the '
            'source had.'
        ),
        'columns': {
            'target_table_id': 'Identifier of the table the partitioning belongs to.',
            'target_table_partitioning_level': 'The level of the partitioning this row describes - 1 for the partitioning of the table, 2 for the partitioning of its partitions, and so on.',
            'target_partition_columns': 'The columns the target table is partitioned by on this level.',
            'target_partition_ranges': 'The bounds of the partitions created in the target on this level.',
        },
    }

    catalog[config_parser.get_protocol_name_columns()] = {
        'comment': (
            'One row per column - everything the source declares about a column and everything the column became in '
            'the target. The standard workflow keeps the columns of a table as JSON in the source_columns and '
            'target_columns fields of the tables protocol table, so this table stays empty unless a workflow fills it.'
        ),
        'columns': {
            'source_column_name': 'Name of the column in the source table.',
            'source_column_id': 'Position of the column in the source table.',
            'source_column_data_type': 'The type of the column as the source declares it.',
            'source_column_is_nullable': 'Whether the source allows NULL in the column, as the source catalog reports it.',
            'source_column_is_primary_key': 'Whether the column belongs to the primary key of the source table.',
            'source_column_is_identity': 'Whether the source generates the value of the column itself - an identity or an autoincrement column. Such a column becomes an identity column, or a column with a sequence as its default, in the target.',
            'source_column_default_name': 'Name of the default object bound to the column, for the engines which keep default values as objects of their own - see the defaults protocol table.',
            'source_column_default_value': 'The default value of the column as the source states it.',
            'source_column_replaced_default_value': 'The default value put in its place, when the one of the source cannot be used - a function PostgreSQL does not have, or a value the configuration replaces through data_types_substitution / default_values_substitution.',
            'source_column_character_maximum_length': 'The declared length of a character column of the source.',
            'source_column_numeric_precision': 'The declared precision of a numeric column of the source.',
            'source_column_numeric_scale': 'The declared scale of a numeric column of the source.',
            'source_column_basic_data_type': 'The type behind the declared one, when the column is declared with a user defined type or a domain. It is the type the mapping to PostgreSQL really works on.',
            'source_column_basic_character_maximum_length': 'The length of the type behind the declared one - see source_column_basic_data_type.',
            'source_column_basic_numeric_precision': 'The precision of the type behind the declared one - see source_column_basic_data_type.',
            'source_column_basic_numeric_scale': 'The scale of the type behind the declared one - see source_column_basic_data_type.',
            'source_column_basic_column_type': 'The whole declaration of the type behind the declared one, as the source writes it.',
            'source_column_is_generated_virtual': 'Whether the source computes the value of the column on every read. PostgreSQL has no such column, so it is created as a stored generated column instead.',
            'source_column_is_generated_stored': 'Whether the source computes the value of the column when the row is written.',
            'source_column_generation_expression': 'The expression a generated column is computed from, as the source writes it.',
            'source_column_stripped_generation_expression': 'The same expression prepared for the target - what only the source understands removed or rewritten.',
            'source_column_udt_schema': 'Schema of the user defined type the column is declared with - see the user defined types protocol table.',
            'source_column_udt_name': 'Name of the user defined type the column is declared with.',
            'source_column_domain_schema': 'Schema of the domain the column is declared with - see the domains protocol table.',
            'source_column_domain_name': 'Name of the domain the column is declared with.',
            'source_column_description': 'Comment of the column in the source catalog.',
            'source_column_sql': 'The declaration of the column as the source database states it.',
            'target_table_id': 'Id of the row of the table in the tables protocol table.',
            'target_column_name': 'Name of the column in the target table.',
            'target_column_id': 'Position of the column in the target table.',
            'target_column_data_type': 'The PostgreSQL type the column was created with.',
            'target_column_description': 'Comment written on the column in the target.',
            'target_column_sql': 'The declaration of the column as it was sent to the target.',
        },
    }

    catalog[config_parser.get_protocol_name_data_sources()] = {
        'comment': (
            'The files a table is migrated from, when the data does not come out of a live source database but out of '
            'an export - one row per table and file. file_found = false means the migration of that table has nothing '
            'to read.'
        ),
        'columns': {
            'lob_columns': 'The columns whose values are not in the file itself but in files of their own next to it, named by the file - the usual way an export writes out large objects.',
            'file_name': 'The file the data of the table is read from, as it was found.',
            'file_size': 'Size of the file in bytes. -1 = the file was not found.',
            'file_lines': 'Number of lines of the file, when it was counted.',
            'file_found': 'Whether the file really exists. False = the table has no data to migrate, whatever the configuration says.',
            'converted_file_name': 'The file the data is really read from, when the original had to be converted first - recoded into UTF-8, for instance.',
            'format_options': 'How the file is to be read, as JSON: the format, the field delimiter, whether the first line is a header, the character set, and the order of the parts of a date. The file itself states none of this, and a wrong date order migrates wrong dates without any error.',
        },
    }

    catalog[config_parser.get_protocol_name_data_migration()] = {
        'comment': (
            'The movement of the data - one row per table, written when the migration of that table is planned and '
            'filled in as it runs. It holds the counts on both sides and how the batches behaved, so a table which is '
            'slow or which did not arrive completely is found here.'
        ),
        'columns': {
            'source_table_rows_all': 'How many rows the source table holds.',
            'source_table_rows_limited': 'How many rows are to be migrated - fewer than source_table_rows_all when the configuration limits the table with a WHERE clause. This is the number target_table_rows is to be compared against.',
            'target_table_rows': 'How many rows really arrived in the target table.',
            'batch_count': 'How many batches the data of the table was moved in.',
            'shortest_batch_seconds': 'Duration of the fastest batch of this table, in seconds.',
            'longest_batch_seconds': 'Duration of the slowest batch of this table, in seconds. A value far above the average points at a batch which had to wait or which carried large objects.',
            'average_batch_seconds': 'Average duration of a batch of this table, in seconds.',
            'task_started': 'When the worker began to move the data of this table.',
            'task_completed': 'When it finished. Empty while the table is still being migrated.',
        },
    }

    catalog[config_parser.get_protocol_name_batches_stats()] = {
        'comment': (
            'One row per batch of data really written to the target - the finest record of the data migration. It '
            'splits the time of a batch into reading from the source, transforming the values and writing to the '
            'target, which is what says where a slow migration loses its time.'
        ),
        'columns': {
            'chunk_number': 'The chunk of the table the batch belongs to - see the data chunks protocol table.',
            'batch_number': 'The number of the batch inside its chunk.',
            'batch_start': 'When the batch began.',
            'batch_end': 'When the batch ended.',
            'batch_rows': 'How many rows the batch carried.',
            'batch_seconds': 'How long the whole batch took, in seconds. It is more than the three parts below add up to, the difference being the overhead of the batch itself.',
            'reading_seconds': 'Of the time of the batch, how long the reading from the source took.',
            'transforming_seconds': 'Of the time of the batch, how long the conversion of the values took - the type conversions, and the anonymization when it is switched on.',
            'writing_seconds': 'Of the time of the batch, how long the writing into the target took.',
        },
    }

    catalog[config_parser.get_protocol_name_data_chunks()] = {
        'comment': (
            'The pieces a large table is split into so that several workers can move it at the same time - one row per '
            'chunk. Each chunk is a range of rows of the source, and the batches which really carry the data are '
            'counted inside it.'
        ),
        'columns': {
            'source_table_rows_all': 'How many rows the source table holds altogether.',
            'source_table_rows_limited': 'How many rows of the source table are to be migrated.',
            'target_table_rows': 'How many rows the target table holds.',
            'chunk_number': 'The number of this chunk within the table.',
            'chunk_size': 'How many rows the chunk covers.',
            'migration_limitation': 'The WHERE clause the configuration limits the table with. Empty when the whole table is migrated.',
            'chunk_start': 'The first row of the chunk, counted in the order of order_by_clause.',
            'chunk_end': 'The last row of the chunk.',
            'order_by_clause': 'The order the rows are read in. Without a fixed order the chunks would overlap or leave rows out, so it is recorded with the chunk.',
            'inserted_rows': 'How many rows of the chunk really reached the target.',
            'batch_size': 'How many rows are carried in one batch of this chunk.',
            'total_batches': 'How many batches the chunk is moved in.',
            'task_started': 'When the worker began with the chunk.',
            'task_completed': 'When the chunk was finished. Empty while it is still running.',
        },
    }

    catalog[config_parser.get_protocol_name_anonymization_stats()] = {
        'comment': (
            'What the anonymization really did - one row per table, column and method, with the number of values it '
            'actually replaced. The summary of a run reports out of this table, so a rule which never touched a value '
            'cannot be presented as a job which was done.'
        ),
        'columns': {
            'column_name': 'The column the rule was applied to.',
            'method_name': 'The anonymization method which was applied.',
            'params': 'The parameters the method was called with, as they are written in the configuration.',
            'values_anonymized': 'How many values the method really replaced. 0 on a configured rule means the rule never fired - the column was empty, or it was never reached.',
            'values_truncated': 'How many replaced values had to be cut off because they did not fit into the target column.',
            'values_refitted': 'How many replaced values had to be reshaped to satisfy the target column - its length, its precision or its type.',
            'table_rows': 'How many rows of the table went through the anonymization.',
        },
    }

    catalog[config_parser.get_protocol_name_pk_ranges()] = {
        'comment': (
            'Ranges of the primary key handed to the workers, for the tables whose rows are split by their key instead '
            'of by their position. The analysis writing it is switched off at the moment, so the table is normally '
            'empty.'
        ),
        'columns': {
            'pk_columns': 'The columns of the primary key the range is expressed in.',
            'batch_start': 'The first key value of the range.',
            'batch_end': 'The last key value of the range.',
            'row_count': 'How many rows the range holds.',
        },
    }

    catalog[config_parser.get_protocol_name_indexes()] = {
        'comment': (
            'The indexes of the source and the indexes created for them in the target. They are created after the data '
            'was migrated, which is why a table can hold all of its rows while its indexes are still missing.'
        ),
        'columns': {
            'index_owner': 'Owner of the index in the source database.',
            'index_name': 'Name of the index in the source database. PostgreSQL requires index names to be unique within a schema while several sources require them to be unique only within a table, so the name in the target can carry the name of the table.',
            'target_index_name': 'The name the object really has in the target - the spelling names_case_handling produced. The column next to it holds the spelling of the source, which is kept unchanged: two objects of the source which differ only in the case of their letters are two different objects, and the record of what was read has to say so.',
            'index_type': 'The kind of index - unique, clustered, bitmap and so on, as the source calls it.',
            'index_sql': 'The CREATE INDEX statement which was sent to the target.',
            'index_columns': 'The columns the index is built on, in their order.',
            'index_comment': 'Description of the index read from the source catalog.',
            'is_function_based': 'Whether the index is built on an expression instead of on plain columns. Such an index needs the expression to be converted as well, so it fails for reasons an ordinary index cannot.',
        },
    }

    catalog[config_parser.get_protocol_name_constraints()] = {
        'comment': (
            'The constraints of the source - primary keys, unique, foreign key and check constraints - and what was '
            'created for them in the target. They are created after the data was migrated, so a constraint which '
            'failed here usually points at data the source itself no longer satisfies.'
        ),
        'columns': {
            'constraint_name': 'Name of the constraint in the source database.',
            'target_constraint_name': 'The name the object really has in the target - the spelling names_case_handling produced. The column next to it holds the spelling of the source, which is kept unchanged: two objects of the source which differ only in the case of their letters are two different objects, and the record of what was read has to say so.',
            'constraint_type': 'The kind of constraint - PRIMARY KEY, UNIQUE, FOREIGN KEY or CHECK.',
            'constraint_owner': 'Owner of the constraint in the source database.',
            'constraint_columns': 'The columns of the table the constraint is placed on.',
            'referenced_table_schema': 'Schema of the table a foreign key points at.',
            'referenced_table_name': 'The table a foreign key points at, named as the source names it.',
            'target_referenced_table_name': 'The name the object really has in the target - the spelling names_case_handling produced. The column next to it holds the spelling of the source, which is kept unchanged: two objects of the source which differ only in the case of their letters are two different objects, and the record of what was read has to say so.',
            'referenced_columns': 'The columns of that table the foreign key points at.',
            'constraint_sql': 'The statement which was sent to the target.',
            'delete_rule': 'What a foreign key does when the referenced row is deleted - NO ACTION, CASCADE, SET NULL and so on.',
            'update_rule': 'What a foreign key does when the referenced key is changed.',
            'constraint_comment': 'Description of the constraint read from the source catalog.',
            'constraint_status': "Whether the source has the constraint switched on ('ENABLED') or switched off. A constraint the source does not enforce may well be violated by the data, and creating it in the target then fails.",
        },
    }

    catalog[config_parser.get_protocol_name_funcprocs()] = {
        'comment': (
            'The functions and the procedures of the source and their conversion to PL/pgSQL. The code of both sides '
            'is kept: what could not be converted is reported as failed and not created in the target, and '
            'target_funcproc_sql is then the point the work by hand starts from.'
        ),
        'columns': {
            'source_funcproc_name': 'Name of the routine in the source database.',
            'source_funcproc_id': 'The identifier the source database gave the routine, for the engines which have one.',
            'source_funcproc_sql': 'The code of the routine as it stands in the source.',
            'target_funcproc_name': 'Name of the routine in the target.',
            'target_funcproc_sql': 'The code produced for the target. When success is false it was not created - it is the converted code as far as the conversion got, kept so that the remaining work can start from it.',
            'funcproc_comment': 'Description of the routine read from the source catalog.',
        },
    }

    catalog[config_parser.get_protocol_name_sequences()] = {
        'comment': (
            'The sequences of the target and where each of them comes from. A sequence of the target has two possible '
            'origins: a sequence of the source, or an identity / autoincrement column, which is what most legacy '
            'engines have instead - source_is_identity says which of the two it is. target_sequence_last_value is the '
            'value the sequence really carries after the data was migrated, and it is what decides whether the first '
            'row inserted after the migration collides with an existing key.'
        ),
        'columns': {
            'sequence_id': 'Number of the sequence within the run. It is the value the tasks table refers to in object_protocol_id for a row of the type sequence.',
            'source_column_name': 'The column the values are generated for, when the sequence belongs to an identity column.',
            'source_column_data_type': 'The type of that column in the source. It decides how far the sequence can count.',
            'source_is_identity': 'true = the sequence stands for an identity or autoincrement column of the source, false = the source has a sequence object of its own.',
            'source_next_identity': 'The next value the identity column of the source would have handed out. Values too large for a BIGINT are clamped, and message says so when that happened.',
            'source_sequence_name': 'Name of the sequence in the source database. Empty for an identity column - it has no sequence of its own there.',
            'source_sequence_sql': 'The declaration of the sequence as the source states it.',
            'source_start_value': 'The value the sequence of the source is declared to start at - its START WITH, which is where it begins again after a RESTART. It is not where the sequence stands: that is source_last_value. Empty for a source which does not keep the declared value, Oracle among them.',
            'source_last_value': 'Where the sequence of the source stands - the last value it handed out, or the next one it would hand out for a source which reports only that. This is the value the sequence of the target is positioned to, and it is empty for a source which cannot be asked, an offline DDL delivery for instance.',
            'source_increment_by': 'The step of the sequence of the source.',
            'source_minvalue': 'The lower bound of the sequence of the source.',
            'source_maxvalue': 'The upper bound of the sequence of the source.',
            'source_cache': 'How many values the source hands out in advance. A cache means the source can have skipped values, which is why the last value of the target is set from the data and not from this.',
            'source_is_cycled': 'Whether the sequence of the source starts again from its minimum when it reaches its maximum.',
            'source_sequence_comment': 'Description of the sequence read from the source catalog.',
            'target_column_name': 'The column of the target the sequence feeds.',
            'target_column_data_type': 'The type of that column in the target. A sequence counting further than the column can hold is a migration which fails later, not now.',
            'target_sequence_name': 'Name of the sequence in the target. An identity column of the source gets its sequence named here, since the source had no name for it.',
            'target_sequence_sql': 'The statement which was sent to the target.',
            'target_sequence_last_value': 'The value the sequence of the target really carries after the migration - set from the data which arrived, so that the next insert does not collide with a migrated row.',
            'target_sequence_comment': 'Comment written on the sequence in the target.',
        },
    }

    catalog[config_parser.get_protocol_name_aliases()] = {
        'comment': (
            'The aliases and synonyms of the source - the second names under which a table, a view or a column is also '
            'known. PostgreSQL has no such object, so an alias is created as a view over what it points at, and '
            'alias_target_type says what the source meant by it.'
        ),
        'columns': {
            'source_alias_name': 'The alias as the source database knows it.',
            'source_alias_id': 'The identifier the source database gave the alias, for the engines which have one.',
            'source_alias_sql': 'The declaration of the alias as the source states it.',
            'source_referenced_schema_name': 'Schema of the object the alias points at in the source.',
            'source_referenced_table_name': 'The object the alias points at in the source.',
            'source_referenced_column_name': 'The column the alias points at, when the alias is the second name of a column and not of a table.',
            'source_alias_comment': 'Description of the alias read from the source catalog.',
            'target_alias_name': 'The name the alias was created under in the target.',
            'alias_target_type': "What the alias stands for - the second name of a table or of a view, or the system name a source such as DB2 for i keeps next to the long name ('SYSTEM NAME').",
            'target_referenced_schema_name': 'Schema of the object the alias points at in the target.',
            'target_referenced_table_name': 'The object the alias points at in the target.',
            'target_referenced_column_name': 'The column the alias points at in the target.',
            'target_alias_sql': 'The statement which was sent to the target - the view which carries the alias.',
        },
    }

    catalog[config_parser.get_protocol_name_triggers()] = {
        'comment': (
            'The triggers of the source and their conversion. PostgreSQL runs a trigger through a function of its own, '
            'so one trigger of the source becomes a function and a trigger. A trigger whose code could not be fully '
            'converted is not created and is reported as failed - trigger_target_sql then holds how far the conversion '
            'got, for the work by hand.'
        ),
        'columns': {
            'trigger_id': 'The identifier the source database gave the trigger, for the engines which have one.',
            'trigger_name': 'Name of the trigger in the source database.',
            'target_trigger_name': 'The name the object really has in the target - the spelling names_case_handling produced. The column next to it holds the spelling of the source, which is kept unchanged: two objects of the source which differ only in the case of their letters are two different objects, and the record of what was read has to say so.',
            'trigger_event': 'What fires the trigger - INSERT, UPDATE, DELETE, and whether it runs before or after the statement.',
            'trigger_new': 'The name the source gives the new image of the row. In the target it is read as NEW.',
            'trigger_old': 'The name the source gives the old image of the row. In the target it is read as OLD.',
            'trigger_row_statement': 'Whether the trigger runs once per row or once per statement. Several sources only have statement level triggers, which read their rows from tables of their own, and those cannot be turned into row level triggers.',
            'trigger_source_sql': 'The code of the trigger as it stands in the source.',
            'trigger_target_sql': 'The code produced for the target. When success is false it was not created - it is what the conversion managed, kept so that the rest can be written by hand.',
            'trigger_comment': 'Description of the trigger read from the source catalog.',
            'requires_manual_adjustment': 'true = the trigger was created, but it does not do everything the source did and has to be looked at. What is missing stands in manual_adjustment_details.',
            'manual_adjustment_details': 'What the conversion could not express and what has to be added by hand.',
        },
    }

    catalog[config_parser.get_protocol_name_queries()] = {
        'comment': (
            'The statements of an application which the query conversion read, and what became of each of them. '
            'The step runs over a finished migration and writes nothing to either database - this table is the '
            'record of what it found. status says it in one word: CONVERTED (changed and accepted by the target), '
            'UNCHANGED (already valid PostgreSQL), CONVERTED_FAILING (converted, and the target refused it), '
            'NOT CONVERTED (the converter could not do it) and SKIPPED (a gate refused it, because it is not a read).'
        ),
        'columns': {
            'input_file': 'The file the statement was read from.',
            'statement_ordinal': 'Its place in that file, counted from 1.',
            'line_from': 'The line the statement begins at, so it can be found in the file it came from.',
            'line_to': 'The line it ends at.',
            'statement_name': "The name written above the statement as '-- name: ...', when it carries one.",
            'statement_hash': 'The hash of the statement with its whitespace normalised. The same statement written twice is converted and tested once, and a later run can tell what changed.',
            'status': 'What became of the statement - one of CONVERTED, UNCHANGED, CONVERTED_FAILING, NOT CONVERTED, SKIPPED.',
            'reason': 'Why, for everything which is not simply converted: which gate refused the statement, or what the converter or the target said.',
            'source_sql': 'The statement as it stands in the file of the application.',
            'target_sql': 'The statement as it was written into the output file, with the bind parameter markers of the application back in it.',
            'source_test_result': 'The outcome of compiling the statement against the SOURCE database before it was converted - OK, FAILED, ERROR or "not run". FAILED says the statement was already broken, or reads an object the application creates at run time, which is not a failure of the conversion. Compile only: PREPARE, EXPLAIN, SET NOEXEC ON or the prepareStatement of a JDBC driver, never an execution. Switched with query_conversion.source_test.',
            'source_test_message': 'What the source answered, or why it was not asked.',
            'target_test_result': "OK, FAILED, INCONCLUSIVE or 'not run'. INCONCLUSIVE means PostgreSQL could not infer the type of a bind parameter, which says nothing about the rest of the statement.",
            'target_test_message': 'What the target answered - the error of a statement it refused, or which test was run.',
            'target_test_duration_ms': 'How long the target needed for it. It is here and not in the output file, so that the file stays the same for the same input.',
            'warnings': 'What has to be read before the statement is used, one per line. A warning marked BLOCKING says the converted statement must not be used as it stands.',
            'identical_to': 'The ordinal of the statement this one repeats, when the same statement stands in the file more than once. Such a statement is converted and tested once.',
            'success': 'true for a statement which was converted or was already valid and passed the test of the target; false for everything else.',
        },
    }

    catalog[config_parser.get_protocol_name_views()] = {
        'comment': (
            'The views of the source and the views created for them in the target. A view is only valid once '
            'everything it selects from exists, so views are created after the tables and checked again at the end of '
            'the run - see final_valid.'
        ),
        'columns': {
            'source_view_name': 'Name of the view in the source database.',
            'source_view_id': 'The identifier the source database gave the view, for the engines which have one.',
            'source_view_sql': 'The query of the view as it stands in the source.',
            'target_view_name': 'Name of the view in the target.',
            'target_view_alias': 'The second name the view is also reachable under in the target. Empty when there is none.',
            'target_view_sql': 'The CREATE VIEW statement which was sent to the target.',
            'alias_view': 'true = the row is not a view of the source at all but a view created to carry an alias of a table - see the aliases protocol table.',
            'view_comment': 'Description of the view read from the source catalog and carried over to the target.',
        },
    }

    catalog['mapping_pre_stats'] = {
        'comment': (
            'How many objects of each kind each of the two sides holds, counted before they are matched. It is the '
            'measure the result of the matching is read against: 40 matched tables mean something different against 42 '
            'than against 400.'
        ),
        'columns': {
            'id': 'Primary key of the record.',
            'side': "Which database was counted - 'source' or 'target'.",
            'object_type': 'The kind of object which was counted - tables, indexes, constraints.',
            'object_count': 'How many of them that side holds.',
        },
    }

    catalog['mapping_tables'] = {
        'comment': (
            'The result of matching the tables of the source against the tables of an already existing target - one '
            'row per pair which was matched. This is used when the target is not created by the migrator but is '
            'already there, and the migration has to find out which table of the target belongs to which table of the '
            'source. match_type says how a pair was found, and a pair found by similarity deserves a look before the '
            'data is moved.'
        ),
        'columns': {
            'id': 'Primary key of the record.',
            'match_type': "How the pair was found: 'Internal Mapping' - the two are tied together by the properties of the objects; 'Exact Name' - the names are the same; 'Normalized Name' - they are the same after the naming rules of the configuration were applied; 'Column Fingerprint' - the names differ and the columns matched; 'Forced Exact' / 'Forced Regex Sub' - the configuration prescribes the pair.",
            'similarity_score': 'How certain the match is, from 0 to 100. 100 = the names are the same or the configuration prescribes it; a lower value comes from the similarity of the columns and is worth checking.',
            'source_table_rows_all': 'How many rows the source table holds.',
            'source_table_rows_limited': 'How many rows of it are to be migrated.',
            'target_table_rows': 'How many rows the matched table of the target holds. A target table which is not empty before the migration is a warning.',
            'info': 'What the match was made on, as JSON - the evidence and the intermediate measurements of the matching.',
            'is_forced_mapping': 'true = the pair does not come from the matching at all but is prescribed in the configuration.',
        },
    }

    catalog['mapping_columns'] = {
        'comment': (
            'The result of matching the columns of a matched pair of tables - one row per pair of columns. The order '
            'of the columns of the two sides is not necessarily the same, and this is what says which column of the '
            'target a value of the source is written into.'
        ),
        'columns': {
            'id': 'Primary key of the record.',
            'source_column_name': 'The column of the source table.',
            'target_column_name': 'The column of the target table it was matched with.',
            'source_ordinal_number': 'Position of the column in the source table.',
            'target_ordinal_number': 'Position of the column in the target table. It does not have to be the same as on the source side.',
            'source_data_type': 'The type of the column in the source.',
            'target_data_type': 'The type of the column in the target. A pair whose types do not fit is where the data migration fails.',
            'match_type': 'How the pair of columns was found - by the name, by the name after the naming rules of the configuration were applied, or by the position and the type.',
            'source_is_identity': 'Whether the source generates the value of this column itself.',
            'target_is_identity': 'Whether the target generates the value of this column itself. A column the target generates cannot simply be written into - the value of the source has to be forced in, or the sequence set afterwards.',
        },
    }

    catalog['mapping_target_indexes'] = {
        'comment': (
            'The indexes which already exist in the target, read before the data is migrated. Writing into an indexed '
            'table is considerably slower, so the indexes are dropped for the migration and created again afterwards - '
            'index_def is the definition kept for exactly that, and dropped and success say where in that cycle an '
            'index stands.'
        ),
        'columns': {
            'id': 'Primary key of the record.',
            'index_name': 'Name of the index in the target.',
            'index_def': 'The definition of the index as the target states it. It is what the index is recreated from after the data was migrated - a row whose index was dropped and whose definition is missing is an index which is lost.',
            'is_primary_key': 'Whether the index carries the primary key of the table.',
            'index_type': 'The kind of index - unique, partial, expression based and so on.',
            'dropped': 'true = the index was dropped for the migration and has to be created again, false = it was left in place.',
            'success': 'Whether the index was created again successfully after the data was migrated: true = yes, false = the target refused it and the answer stands in message, empty = it was not attempted yet.',
            'message': 'The error the target answered with while dropping or recreating the index.',
        },
    }

    catalog['mapping_target_constraints'] = {
        'comment': (
            'The constraints which already exist in the target, read before the data is migrated. They are dropped for '
            'the migration - a foreign key would refuse the rows whose counterpart has not arrived yet - and created '
            'again afterwards from constraint_def.'
        ),
        'columns': {
            'id': 'Primary key of the record.',
            'constraint_name': 'Name of the constraint in the target.',
            'constraint_type': 'The kind of constraint - PRIMARY KEY, UNIQUE, FOREIGN KEY or CHECK.',
            'constraint_def': 'The definition of the constraint as the target states it. It is what the constraint is recreated from after the data was migrated.',
            'dropped': 'true = the constraint was dropped for the migration and has to be created again, false = it was left in place.',
            'success': 'Whether the constraint was created again successfully: true = yes, false = the target refused it - which usually means the migrated data violates it - empty = it was not attempted yet.',
            'message': 'The error the target answered with while dropping or recreating the constraint.',
        },
    }

    catalog['mapping_target_sequences'] = {
        'comment': (
            'The sequences of an existing target and what uses them - one row per place a sequence is used. A sequence '
            'has to be set past the migrated data afterwards, and this says which sequence belongs to which column, '
            'whether it is reached through a default, an identity column or a trigger.'
        ),
        'columns': {
            'id': 'Primary key of the record.',
            'sequence_schema_name': 'Schema of the sequence in the target.',
            'sequence_name': 'Name of the sequence in the target.',
            'used_in_default': 'true = the column reaches the sequence through its DEFAULT clause.',
            'used_in_identity': 'true = the column is an identity column and the sequence belongs to it.',
            'used_in_trigger': 'true = a trigger fetches the value out of the sequence. The column itself says nothing about the sequence in that case, which is why it is recorded here.',
            'trigger_name': 'The trigger which uses the sequence, when it is used through one.',
            'column_name': 'The column which receives the values of the sequence.',
            'source_sequence_schema_name': 'Schema of the sequence of the source this one corresponds to.',
            'source_sequence_name': 'The sequence of the source this one corresponds to.',
        },
    }

    catalog['mapping_unmatched_objects'] = {
        'comment': (
            'Everything the matching against an existing target could not pair up - the tables and the columns which '
            'exist only on one of the two sides. A source object standing here is data which will not be migrated; a '
            'target object standing here is a column or a table the migration will not fill.'
        ),
        'columns': {
            'id': 'Primary key of the record.',
            'object_type': "What was not matched - 'table' or 'column'.",
            'side': "Which side it exists on - 'source' (it is not migrated) or 'target' (nothing is written into it).",
            'parent_object': 'The table an unmatched column belongs to. Empty for an unmatched table.',
            'object_name': 'Name of the unmatched object.',
            'row_count': 'How many rows an unmatched table holds. It says how much data is at stake.',
            'info': 'What is known about the object, as JSON - what the matching had to work with.',
        },
    }

    ddl_note = (
        'It is filled by the connectors which migrate a source from its DDL export instead of from a live connection '
        '(DB2 for i, DB2 for z/OS): the script files are parsed once and the ddl_* tables then take the place of the '
        'catalog of the source for the whole run.'
    )

    catalog['ddl_tables'] = {
        'comment': f'The tables found in the DDL of the source. {ddl_note}',
        'columns': {
            'source_schema_name': 'Schema of the table, as the parsed DDL spells it. The ddl_* tables are looked up by exactly this spelling.',
            'source_table_name': 'Name of the table, as the parsed DDL spells it.',
            'source_partition_columns': 'The columns the table is partitioned by, when its DDL partitions it.',
            'source_partition_ranges': 'The bounds of the partitions stated in the DDL.',
            'source_table_sql': 'The CREATE TABLE statement as it stands in the DDL file.',
            'source_table_comment': 'The comment which belongs to the table - the COMMENT ON of the DDL, or the comment written above the statement.',
        },
    }

    catalog['ddl_columns'] = {
        'comment': f'The columns found in the DDL of the source - one row per column of a table of ddl_tables. {ddl_note}',
        'columns': {
            'source_schema_name': 'Schema of the table the column belongs to, as the parsed DDL spells it.',
            'source_table_name': 'The table the column belongs to.',
            'source_column_name': 'Name of the column.',
            'source_data_type': 'The type of the column as the DDL declares it, with its length and its precision.',
            'source_is_nullable': 'Whether the DDL allows NULL in the column.',
            'source_default_value': 'The default value stated in the DDL.',
            'source_pk_indicator': 'Whether the column belongs to the primary key of the table.',
            'source_is_identity': 'Whether the DDL has the value of the column generated by the database.',
            'source_column_sql': 'The declaration of the column as it stands in the DDL file.',
            'source_column_comment': 'The comment which belongs to the column.',
        },
    }

    catalog['ddl_indexes'] = {
        'comment': f'The indexes found in the DDL of the source. {ddl_note}',
        'columns': {
            'source_schema_name': 'Schema of the indexed table, as the parsed DDL spells it.',
            'source_table_name': 'The table the index belongs to.',
            'source_index_name': 'Name of the index.',
            'source_is_unique': 'Whether the index is unique.',
            'source_columns_list': 'The columns of the index, in their order.',
            'source_index_sql': 'The CREATE INDEX statement as it stands in the DDL file.',
            'source_index_comment': 'The comment which belongs to the index.',
            'source_is_function_based': 'Whether the index is built on an expression instead of on plain columns - such an index needs its expression converted as well.',
        },
    }

    catalog['ddl_foreign_keys'] = {
        'comment': (
            'The table constraints found in the DDL of the source. Despite its name the table holds every kind of '
            f'constraint - source_constraint_type says which one a row is. {ddl_note}'
        ),
        'columns': {
            'source_schema_name': 'Schema of the constrained table, as the parsed DDL spells it.',
            'source_table_name': 'The table the constraint belongs to.',
            'source_fk_name': 'Name of the constraint.',
            'source_columns_list': 'The columns of the table the constraint is placed on.',
            'source_ref_schema_name': 'Schema of the table a foreign key points at.',
            'source_ref_table_name': 'The table a foreign key points at.',
            'source_ref_columns_list': 'The columns of that table the foreign key points at.',
            'source_fk_sql': 'The statement which declares the constraint, as it stands in the DDL file.',
            'source_fk_comment': 'The comment which belongs to the constraint.',
            'source_constraint_type': "Which kind of constraint the row really is - 'FOREIGN KEY', 'PRIMARY KEY', 'UNIQUE' or 'CHECK'.",
            'source_check_clause': 'The condition of a check constraint. Empty for the other kinds.',
            'source_delete_rule': 'What a foreign key does when the referenced row is deleted - NO ACTION, CASCADE, SET NULL and so on.',
            'source_update_rule': 'What a foreign key does when the referenced key is changed.',
        },
    }

    catalog['ddl_sequences'] = {
        'comment': f'The sequences found in the DDL of the source, with the column each of them feeds. {ddl_note}',
        'columns': {
            'source_schema_name': 'Schema of the sequence, as the parsed DDL spells it.',
            'source_seq_name': 'Name of the sequence.',
            'source_table_name': 'The table which uses the sequence, when the DDL ties it to one.',
            'source_column_name': 'The column which receives its values.',
            'source_start_value': 'The value the sequence is declared to start at, as the parsed DDL states it. A DDL file says nothing about where the sequence stood in the database it was taken from.',
            'source_increment_by': 'The step of the sequence.',
            'source_minvalue': 'The lower bound of the sequence.',
            'source_maxvalue': 'The upper bound of the sequence.',
            'source_cache': 'How many values the source hands out in advance.',
            'source_is_cycled': 'Whether the sequence starts again from its minimum when it reaches its maximum.',
            'source_ddl_text': 'The statement which declares the sequence, as it stands in the DDL file.',
            'source_seq_comment': 'The comment which belongs to the sequence.',
        },
    }

    catalog['ddl_views'] = {
        'comment': f'The views found in the DDL of the source. {ddl_note}',
        'columns': {
            'source_schema_name': 'Schema of the view, as the parsed DDL spells it.',
            'source_view_name': 'Name of the view.',
            'source_view_sql': 'The CREATE statement of the view as it stands in the DDL file.',
            'source_view_comment': 'The comment which belongs to the view.',
            'source_view_type': "Whether the object is an ordinary view ('VIEW') or a materialized one, which holds its rows and is migrated as a table with a refresh.",
        },
    }

    catalog['ddl_aliases'] = {
        'comment': (
            'The aliases and synonyms found in the DDL of the source, together with what was created for them in the '
            f'target. {ddl_note}'
        ),
        'columns': {
            'source_schema_name': 'Schema of the alias, as the parsed DDL spells it.',
            'source_alias_name': 'The alias as the DDL declares it.',
            'source_target_schema': 'Schema of the object the alias points at.',
            'source_target_name': 'The object the alias points at.',
            'source_alias_sql': 'The statement which declares the alias, as it stands in the DDL file.',
            'source_alias_comment': 'The comment which belongs to the alias.',
            'alias_target_type': "What the alias stands for - the second name of a table or of a view, or the system name a source such as DB2 for i keeps next to the long name ('SYSTEM NAME').",
            'target_schema_name': 'Schema the alias was created in in the target.',
            'target_alias_name': 'The name the alias was created under in the target.',
            'target_referenced_schema_name': 'Schema of the object the alias points at in the target.',
            'target_referenced_table_name': 'The object the alias points at in the target.',
            'target_referenced_column_name': 'The column the alias points at in the target.',
        },
    }

    catalog['ddl_triggers'] = {
        'comment': f'The triggers found in the DDL of the source, with their whole code. {ddl_note}',
        'columns': {
            'source_schema_name': 'Schema of the trigger, as the parsed DDL spells it.',
            'source_trigger_name': 'Name of the trigger.',
            'source_ddl_text': 'The statement which declares the trigger, as it stands in the DDL file.',
            'source_trigger_sql': 'The code of the trigger which the conversion works on.',
            'source_trigger_comment': 'The comment which belongs to the trigger.',
        },
    }

    catalog['ddl_funcprocs'] = {
        'comment': f'The functions and the procedures found in the DDL of the source, with their whole code. {ddl_note}',
        'columns': {
            'source_schema_name': 'Schema of the routine, as the parsed DDL spells it.',
            'source_funcproc_name': 'Name of the routine.',
            'source_funcproc_type': 'Whether the routine is a function or a procedure. The two are created differently in PostgreSQL and are called differently.',
            'source_ddl_text': 'The code of the routine as it stands in the DDL file.',
            'source_funcproc_comment': 'The comment which belongs to the routine.',
        },
    }

    catalog['ddl_variables'] = {
        'comment': (
            'The global variables found in the DDL of the source - objects DB2 keeps in its catalog and which a '
            f'routine reads like a column. PostgreSQL has no counterpart for them. {ddl_note}'
        ),
        'columns': {
            'source_schema_name': 'Schema of the variable, as the parsed DDL spells it.',
            'source_variable_name': 'Name of the variable. It is the name the code of the source reads.',
            'source_data_type': 'The type of the variable.',
            'source_default_value': 'The value the variable holds until something sets it.',
            'source_variable_sql': 'The statement which declares the variable, as it stands in the DDL file.',
            'source_variable_comment': 'The comment which belongs to the variable.',
        },
    }

    catalog['data_types_substitution'] = {
        'comment': (
            'The type replacements from the configuration - the places where the standard mapping of the migrator is '
            'overruled. Every column of the migration is looked up here before its type is decided; the first entry '
            'which matches wins.'
        ),
        'columns': {
            'table_name': 'The table the entry applies to. Empty = every table. The value is compared as a name, as a regular expression and with ILIKE, so one entry can cover a whole group of tables.',
            'column_name': 'The column the entry applies to. Empty = every column. It is compared the same way as table_name.',
            'source_type': 'The type of the source the entry replaces. Compared as a name, as a regular expression and with ILIKE.',
            'target_type': 'The PostgreSQL type used instead of what the standard mapping would have chosen.',
            'comment': 'Why the replacement is configured, as it is written in the configuration.',
            'inserted': 'When the entry was read out of the configuration. The table is rewritten at the beginning of every run.',
        },
    }

    catalog['data_migration_limitation'] = {
        'comment': (
            'The restrictions from the configuration on which rows of a table are migrated. A table listed here is '
            'migrated only in part - the counts of the run are then measured against the restricted number and not '
            'against the whole table.'
        ),
        'columns': {
            'source_table_name': 'The table the restriction applies to. It is compared as a name and as a regular expression, so one entry can cover a group of tables.',
            'where_limitation': 'The condition added to the reading of the table - only the rows satisfying it are migrated.',
            'use_when_column_present': 'The column which has to exist for the restriction to be used. It lets one condition be written for a group of tables while it only applies to those which really have the column.',
            'row_limit': 'The number of rows a table has to exceed before the restriction is used on it - it leaves the small tables complete and thins out the large ones. Empty means the restriction is used whatever the size of the table.',
            'inserted': 'When the entry was read out of the configuration. The table is rewritten at the beginning of every run.',
        },
    }

    catalog['remote_objects_substitution'] = {
        'comment': (
            'The replacements from the configuration for objects which do not live in the migrated database at all - '
            'the references to another server or another database which the code of the source carries. They are '
            'rewritten to the name the object has in the new landscape.'
        ),
        'columns': {
            'source_object_name': 'The name as the code of the source writes it.',
            'target_object_name': 'The name it is rewritten to.',
            'inserted': 'When the entry was read out of the configuration. The table is rewritten at the beginning of every run.',
        },
    }

    catalog['default_values_substitution'] = {
        'comment': (
            'The replacements from the configuration for the default values of columns - for the defaults of the '
            'source which PostgreSQL cannot take over as they are written, a function the source has and the target '
            'does not being the usual case.'
        ),
        'columns': {
            'column_name': 'The column the entry applies to. Empty = every column.',
            'source_column_data_type': 'The type of the column the entry applies to. Empty = every type.',
            'default_value_value': 'The default value of the source which is replaced.',
            'target_default_value': 'What is written into the DEFAULT clause of the target column instead.',
            'inserted': 'When the entry was read out of the configuration. The table is rewritten at the beginning of every run.',
        },
    }

    catalog[config_parser.get_validation_tables_name()] = {
        'comment': (
            'The result of the validation at the table level - one row per migrated table. It compares what the source '
            'holds with what really arrived: the number of rows, the number of columns, indexes and constraints, and a '
            'hash over the whole content. It is the closing evidence that a table was migrated completely.'
        ),
        'columns': {
            'source_row_count': 'How many rows the source table holds - the rows to be migrated, when the configuration restricts the table.',
            'target_row_count': 'How many rows the target table holds.',
            'source_table_hash': 'A hash over the content of the source table. Two tables with the same number of rows can still hold different values, and this is what catches that.',
            'target_table_hash': 'The same hash computed over the target table. It can only be compared when the values of both sides can be brought into the same form.',
            'source_columns_count': 'How many columns the source table has.',
            'target_columns_count': 'How many columns the target table has.',
            'source_indexes_count': 'How many indexes the source table has.',
            'target_indexes_count': 'How many indexes the target table has. More than the source is normal - PostgreSQL creates an index for every primary key and every unique constraint, and the migration adds one to the parent side of a foreign key which has none - so the two are compared for a SHORTFALL and not for equality: fewer means an index of the source is not here.',
            'source_constraints_count': 'How many constraints the source table has. The engines do not count the same things (the SQLite connector counts neither the primary key nor a unique constraint), which is the other reason the comparison is a shortfall and not an equality.',
            'target_constraints_count': 'How many constraints the target table has.',
            'columns_count_passed': 'Whether the target table has the columns of the source. This one IS compared exactly: a column fewer is data which did not arrive.',
            'indexes_count_passed': 'Whether the indexes of the source are all in the target. Until 0.16.0 the four counts beside it were recorded and compared by nothing, so a table which arrived with half its indexes was reported as validated.',
            'constraints_count_passed': 'Whether the constraints of the source are all in the target.',
            'row_count_passed': 'Whether the two row counts agree. PASS, X for a mismatch, SKIP where the check ran and could not decide, and - where it was never asked for.',
            'table_hash_passed': 'Whether the two hashes agree, or why the comparison could not be made.',
            'row_hash_passed': 'Whether the sample of rows compared row by row agrees. It could fail a table in the log and was written into no column at all until 0.16.0, so a table which failed it was shown as passed in the summary.',
            'lob_size_passed': 'Whether the sizes of the large objects of the sampled rows agree. Recorded since 0.16.0, for the same reason as the column before it.',
            'validation_outcome': "What the validation of this table ended in: PASSED, FAILED, or NOT VALIDATED. The third one is not a failure and not a pass - it means not one check could be run against the table (no primary key, no checksum on that source, the checks switched off), so the run says nothing about whether the table is correct. Before 0.16.0 such a table was reported exactly like one which passed every check.",
            'validation_message': 'What was checked and what each check said - and, for a table which could not be measured, why each check could not run.',
            'validated_at': 'When this comparison was made.',
        },
    }

    catalog[config_parser.get_validation_columns_name()] = {
        'comment': (
            'The result of the validation at the column level - one row per migrated column. It goes past the number '
            'of rows into the values themselves: the hash of the column, the number of NULLs and of empty strings, and '
            'the smallest, the largest and the average value. It is where a type conversion which silently changed the '
            'data becomes visible.'
        ),
        'columns': {
            'source_column_name': 'The column of the source.',
            'target_column_name': 'The column of the target it was compared with.',
            'source_data_type': 'The type of the column in the source.',
            'target_data_type': 'The type of the column in the target.',
            'source_precision': 'The precision the source declares for the column.',
            'target_precision': 'The precision the target declares. A smaller precision on the target side means values were rounded.',
            'source_hash': 'A hash over all values of the source column.',
            'target_hash': 'The same hash over the target column. Two different hashes on equal row counts mean the values were changed on the way.',
            'source_null_count': 'How many NULLs the source column holds.',
            'target_null_count': 'How many NULLs the target column holds. More NULLs on the target side means values were lost.',
            'source_empty_string_count': "How many empty strings the source column holds. It matters for sources which do not tell an empty string and a NULL apart - Oracle stores '' as NULL - where the two counts move into each other during a migration.",
            'target_empty_string_count': 'How many empty strings the target column holds.',
            'source_min_value': 'The smallest value of the source column.',
            'target_min_value': 'The smallest value of the target column.',
            'source_max_value': 'The largest value of the source column. Together with the target side it shows a value which was cut off or overflowed.',
            'target_max_value': 'The largest value of the target column.',
            'source_avg_value': 'The average of the source column, for the columns an average can be computed for.',
            'target_avg_value': 'The average of the target column. A difference on equal minimum and maximum values points at rounding.',
            'source_row_count': 'How many rows the comparison of the source column covered.',
            'target_row_count': 'How many rows the comparison of the target column covered.',
            'passed': 'Whether everything compared for this column agrees.',
        },
    }

    catalog[config_parser.get_validation_indexes_name()] = {
        'comment': (
            'The result of the validation of the indexes - one row per index which was compared. An index which is '
            'missing costs no data but a great deal of speed, and one whose columns differ does not serve the queries '
            'the source served.'
        ),
        'columns': {
            'source_index_name': 'The index in the source.',
            'target_index_name': 'The index in the target it was compared with. Empty = it is missing there.',
            'source_index_type': 'The kind of index in the source.',
            'target_index_type': 'The kind of index in the target. The kinds do not always have a counterpart, so a difference is not necessarily a fault.',
            'source_index_columns': 'The columns of the index in the source, in their order.',
            'target_index_columns': 'The columns of the index in the target. The order matters - an index in a different order serves different queries.',
            'passed': 'Whether the two indexes agree.',
        },
    }

    catalog[config_parser.get_validation_constraints_name()] = {
        'comment': (
            'The result of the validation of the constraints - one row per constraint which was compared. A constraint '
            'which is missing in the target is a rule the data is no longer held to.'
        ),
        'columns': {
            'source_constraint_name': 'The constraint in the source.',
            'target_constraint_name': 'The constraint in the target it was compared with. Empty = it is missing there.',
            'source_constraint_type': 'The kind of constraint in the source - PRIMARY KEY, UNIQUE, FOREIGN KEY or CHECK.',
            'target_constraint_type': 'The kind of constraint in the target.',
            'source_constraint_columns': 'The columns the constraint is placed on in the source.',
            'target_constraint_columns': 'The columns it is placed on in the target.',
            'passed': 'Whether the two constraints agree.',
        },
    }

    return catalog
