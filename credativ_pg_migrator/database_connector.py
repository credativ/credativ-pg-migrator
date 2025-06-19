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

from abc import ABC, abstractmethod

class DatabaseConnector(ABC):
    """
    Abstract base class for database connectors.
    Each specific DB implementation must implement these methods.
    """

    def __init__(self, config_parser, source_or_target):
        self.connection = None
        self.config_parser = config_parser
        self.source_or_target = source_or_target

    @abstractmethod
    def connect(self):
        """Establishes a connection to the database."""
        pass

    @abstractmethod
    def disconnect(self):
        """Closes the connection to the database."""
        pass

    @abstractmethod
    def get_sql_functions_mapping(self, settings):
        """
        settings - dictionary with the following keys
            - target_db_type: str - target database type
        Maps SQL functions from the source database to the corresponding SQL functions in the target database.
        Example:
        { 'suser_name': 'current_user',
          'getdate': 'current_timestamp',
          '@@nestlevel': None,
          ...
        }
        If the function is not supported in the target database, it is mapped to None.
        If some function is not included in the mapping, it is understood as "function is the same in both databases"
        """
        pass

    @abstractmethod
    def fetch_table_names(self, table_schema: str):
        """
        Fetch a list of table names in the specified schema.
        Returns:
        { ordinary_number: {
            'id': table_id,
            'schema_name': schema_name,
            'table_name': table_name,
            'comment': table_comment
            }
        }
        """
        pass

    @abstractmethod
    def get_table_description(self, settings) -> dict:
        """
        settings - dictionary with the following keys
            - table_schema: str,
            - table_name: str,
        Fetch a description of the table returned by the source database.
        Content depends on the database type.
        Added for better observability of the migration process.
        Returns a simple dictionary:
            - 'table_description': description of the table from the source database
        """
        pass

    @abstractmethod
    def fetch_table_columns(self, settings) -> dict:
        """
        settings - dictionary with the following keys
            - table_schema: str,
            - table_name: str,
        Returns a dictionary describing the schema of the specific table
        Items names and values correspond with INFORMATION_SCHEMA.COLUMNS table
        In case of legacy databases, content is suplied from system tables
        Columns starting with 'replaced_*' store substituted values
        Some connectors might add specific columns but these are not recognized by other connectors
        Not all columns are used in all connectors

        { column_ordinary_number: {
            'column_name':
                - full column name, in the format taken from system tables
                - can contain mix of upper and lower case letters as they are stored in system tables
            'is_nullable':
                - 'YES' / 'NO' -> 'NO' = constraint NOT NULL
            'column_default_name':
                - name of the default value from the system tables
                - relevant only for some databases, like Sybase ASE
            'column_default_value':
                - original default value from the system tables
            'replaced_column_default_value':
                - custom replacement for default value
            'data_type':
                - data type without size/length/precision/scale,
            'column_type':
                - full description of data type from table definition with all parameters,
                - like VARCHAR(255) / CHAR(11) / NUMBER(11,2)
                - this value is checked for custom replacements of data types
            'column_type_substitution':
                - custom replacement for column_type - based on the configuration file
                - contains JSON object with key-value pairs based on the configuration file
            'character_maximum_length': length of the column,
            'numeric_precision': numeric precision of the column,
            'numeric_scale': numeric scale of the column,
            'basic_data_type': basic data type for user defined types,
            'basic_character_maximum_length': basic length for user defined types,
            'basic_numeric_precision': basic precision for user defined types,
            'basic_numeric_scale': basic scale for user defined types,
            'basic_column_type': basic column type for user defined types with all parameters,
            'is_identity': 'YES' / 'NO' - automatically generated column from sequence
            'column_comment': comment for the column,
            'is_generated_virtual': 'YES' / 'NO',
            'is_generated_stored': 'YES' / 'NO',
            'generation_expression': expression for generated column,
            'stripped_generation_expression':
                - expression for generated column stripped of all the specific syntax of the source database
            'udt_schema': schema name of the user defined type,
            'udt_name': name of the user defined type,
            'domain_schema':
                - schema name of the domain
                - domains are additional checks on columns
            'domain_name':
                - name of the domain
            'is_hidden_column':
                - 'YES' / 'NO' - hidden column
                - for example hidden calculated stored column in Sybase ASE used for functional indexes
                - it is up to the target database to decide if it is relevant for migration or not
            }
        }

        ## Special notes for some databases:
        # Informix default values: https://www.ibm.com/docs/en/informix-servers/12.10?topic=tables-sysdefaults
        """
        pass

    @abstractmethod
    def fetch_default_values(self, settings) -> dict:
        """
        Relevant only for database that support independently created named default values
        settings - dictionary with the following keys
            - table_schema: str,
        Returns a dictionary describing the default values
        { ordinary_value: {
            - 'default_value_schema':
                - schema name / owner name of the default value
            - 'default_value_name'
            - 'default_value_sql'
                - original source SQL statement to create the default value in the source database
            - 'extracted_default_value':
                - plain default value extracted from the SQL statement
            - 'default_value_data_type':
                - data type of the default value - if possible to easily extract
            - 'default_value_comment'
            }
        }
        """
        pass

    @abstractmethod
    def is_string_type(self, column_type: str) -> bool:
        """
        Check if the column type is a string type.
        Returns True if it is a string type, False otherwise.
        Legacy databases had very different types of string types, therefore this function
        """
        pass

    @abstractmethod
    def is_numeric_type(self, column_type: str) -> bool:
        """
        Check if the column type is a numeric type.
        Returns True if it is a numeric type, False otherwise.
        Legacy databases had very different types of numeric types, therefore this function
        """
        pass

    @abstractmethod
    def get_types_mapping(self, settings):
        """
        settings - dictionary with the following keys
            - target_db_type: str - target database type
        Converts the columns of one source table to the target database type and SQL syntax.
        Returns dictionary of types mapping between source and target database.
        Example:
        { 'INT': 'INTEGER',
          'VARCHAR2': 'VARCHAR',
          'DATETIME': 'TIMESTAMP',
          'CLOB': 'TEXT',
          'BLOB': 'BYTEA',
          ...
        }
        """
        pass

    @abstractmethod
    def get_create_table_sql(self, settings):
        """
        This function is currently relevant only for target database
        Centralizes creation of SQL DDL statement
        settings - dictionary with the following keys
            - target_db_type: str - target database type
            - target_schema: str - schema name of the table in the target database
            - target_table_name: str - table name in the source database
            - source_columns: dict - dictionary of columns to be converted
            - converted_columns: dict - dictionary of converted columns
        Returns:
          - SQL statement to create the table in the database - used for table creation
        """
        pass

    @abstractmethod
    def migrate_table(self, migrate_target_connection, settings):
        """
        Migrate a table from source to target database.
        Procedure is used inside a worker thread.
        Returns number of rows migrated.
        """
        pass

    @abstractmethod
    def fetch_indexes(self, settings):
        """
        Fetch indexes for a table.
        Information_schema on some databases does not contain specific table/view for indexes.
        Therefore columns names in returned dictionary are arbitrary
        settings - dictionary with the following keys
            - source_table_id:
                - internal ID of the table in the source database - if it is available
                - public internal ID does not exist for example in MySQL
            - source_table_schema:
                - schema name of the table in the source database
            - source_table_name:
                - table name in the source database
        Some databases use table_id for finding indexes, some need table_name and schema_name.

        Returned dictionary contains all indexes for the table - both primary and secondary indexes.
        PRIMARY KEYs are usually listed both in the indexes and constraints.
        For our purposes, we include them into indexes, because they should be created before
        references to them are used.

        Returns a dictionary:
            { ordinary_number: {
                'index_name': index_name,
                'index_type': index_type,   # INDEX, UNIQUE, PRIMARY KEY
                'index_owner': index_owner,  ## might be useful for some source databases
                'index_columns':
                    - comma separated, ordered list of columns "column_name1, column_name2, ..."
                    - from some databases like Oracle it might contain also ASC / DESC information for each column
                'index_comment': index_comment
                'index_sql':
                    - Some databases offer directly SQL statement to create the index
                    - if available, it is returned for debugging purposes
                'is_function_based':
                    - 'YES' / 'NO' - if the index is function based
                }
            }

        Notes:
        - 'index_owner':
            some source databases like Informix have a concept of system indexes,
            which are automatically created by the database engine. For example missing primary key index
            on a table if Foreign Key constraint is defined on that column.
            In this case, the owner of the index is set to 'informix' and these indexes might be confusing
            for the user because they are not defined in his data model.
        """
        pass

    @abstractmethod
    def get_create_index_sql(self, settings):
        """
        This function is currently relevant only for target database
        Centralizes creation of SQL DDL statement for indexes
        settings:
            -
        """

    @abstractmethod
    def fetch_constraints(self, settings):
        """
        settings - dictionary with the following keys
            - source_table_id: id of the table in the source database (does not exist in MySQL)
            - source_table_schema: schema name of the table in the source database
            - source_table_name: table name in the source database

        Fetch constraints for a table.
        Returns a dictionary:
            { ordinary_number: {
                'constraint_name': constraint_name:
                'constraint_type': constraint_type,
                'constraint_owner': constraint_owner,
                'constraint_columns':
                    - comma separated, ordered list of columns "column_name1, column_name2, ..."
                'referenced_table_schema':
                    - referenced_table_schema,
                    - might be empty for some databases
                'referenced_table_name': referenced_table_name,
                'referenced_columns':
                    - comma separated, ordered list of columns "column_name1, column_name2, ..."
                'constraint_sql':
                    - in case of foreigh key it might containg full DDL for the constrain from the source database
                    - if available for FK, it is returned for debugging purposes
                    - in case of check constraint in contains check expression
                'delete_rule':
                    - delete rule for foreign key - CASCADE / SET NULL / NO ACTION
                    - available only for some databases
                'update_rule':
                    - update rule for foreign key - CASCADE / SET NULL / NO ACTION
                    - available only for some databases
                'constraint_comment': constraint_comment
                'constraint_status':
                    - status of the constraint - ENABLED / DISABLED
                    - available only for some databases
                }
            }
        """
        pass

    @abstractmethod
    def get_create_constraint_sql(self, settings):
        """
        This function is currently relevant only for target database
        Centralizes creation of SQL DDL statement for constraints
        settings:
            -
        """
        pass

    @abstractmethod
    def fetch_triggers(self, table_id: int, table_schema: str, table_name: str):
        """
        Fetch triggers for a table.
        Returns a dictionary:
            { ordinary_number: {
                'id': trigger_id,
                'name': trigger_name:
                'event': trigger_event,
                'new': referencing_new,
                'old': referencing_old,
                'sql': create_trigger_sql,
                'comment': trigger_comment
                }
            }
        """
        pass

    @abstractmethod
    def convert_trigger(self, trig: str, settings: dict):
        pass

    @abstractmethod
    def fetch_funcproc_names(self, schema: str):
        """
        Fetch function and procedure names in the specified schema.
        Returns: dict
        { ordinary_number: {
            'name': funcproc_name:
            'id': funcproc_id,
            'type': 'FUNCTION' or 'PROCEDURE',
            'comment': funcproc_comment
            }
        }
        """
        pass

    @abstractmethod
    def fetch_funcproc_code(self, funcproc_id: int):
        """
        Fetch the code of a function or procedure.
        Returns a string with the code.
        """
        pass

    @abstractmethod
    def convert_funcproc_code(self, settings):
        """
        settings - dictionary with the following keys:
            - funcproc_code: str - code of the function or procedure in the source database
            - target_db_type: str - target database type
            - source_schema: str - schema name of the function or procedure in the source database
            - target_schema: str - schema name of the function or procedure in the target database
            - table_list: list - list of all tables in the migrated schema
            - view_list: list - list of all views in the migrated schema

        Convert function or procedure to the target database type.
        table_list - contains the list of all tables in the target schema - used for adding target_schema prefix to table names in the function code.
        """
        pass

    @abstractmethod
    def fetch_sequences(self, table_schema: str, table_name: str):
        """
        Fetch sequences for the specified schema and table.
        This function is only relevant for target databases that uses sequences.
        Returns: dict
        { ordinary_number: {
            'name': sequence_name:
            'id': sequence_id,
            'column_name': column_name,
            'set_sequence_sql': set_sequence_sql
            }
        }
        """
        pass

    @abstractmethod
    def get_sequence_details(self, sequence_owner, sequence_name):
        """
        Returns the details of a sequence.
        Returns: dict
        { ordinary_number: {
            'name': sequence_name:
            'min_value': min_value,
            'max_value': max_value,
            'increment_by': increment_by,
            'cycle': cycle,
            'order': order,
            'cache_size': cache_size,
            'last_value': last_value,
            'comment': sequence_comment
            }
        }
        """
        pass

    @abstractmethod
    def fetch_views_names(self, source_schema: str):
        """
        Fetch view names in the specified schema.
        Returns: dict
        { ordinary_number: {
            'id': view_id,
            'schema_name': schema_name,
            'view_name': view_name,
            'comment': view_comment
            }
        }
        """
        pass

    @abstractmethod
    def fetch_view_code(self, settings):
        """
        settings - dictionary with the following keys
            - view_id: id of the view in the source database (does not exist in MySQL)
            - source_schema: schema name of the view in the source database
            - source_view_name: view name in the source database
            - target_schema: target schema name
            - target_view_name: target view name
        Fetch the code of a view.
        Returns a string with the code.
        """
        pass

    @abstractmethod
    def convert_view_code(self, view_code: str, settings: dict):
        """
        Convert view to the target database type.
        table_list - contains the list of all tables in the target schema - used for adding target_schema prefix to table names in the view code.
        """
        pass

    @abstractmethod
    def get_sequence_current_value(self, sequence_id: int):
        """
        Returns the current value of the sequence.
        """
        pass

    @abstractmethod
    def execute_query(self, query: str, params=None):
        """
        Executes a generic query in the connected database.
        """
        pass

    @abstractmethod
    def execute_sql_script(self, script_path: str):
        """Execute SQL script."""
        pass

    @abstractmethod
    def begin_transaction(self):
        """Begins a transaction."""
        pass

    @abstractmethod
    def commit_transaction(self):
        """Commits the current transaction."""
        pass

    @abstractmethod
    def rollback_transaction(self):
        """Rolls back the current transaction."""
        pass

    @abstractmethod
    def get_rows_count(self, table_schema: str, table_name: str):
        """
        Returns a number of rows in a table
        """
        pass

    @abstractmethod
    def get_table_size(self, table_schema: str, table_name: str):
        """
        Returns a size of the table in bytes
        """
        pass

    @abstractmethod
    def fetch_user_defined_types(self, schema: str):
        """
        Returns user defined types in the specified schema / all schemas - depending on the database.
        Returns: dict
        { ordinary_number: {
            'schema_name': schema_name,
            'type_name': type_name,
            'sql': type_sql,
            'comment': type_comment
            }
        }
        """
        pass

    @abstractmethod
    def fetch_domains(self, schema: str):
        """
        Returns domains in the specified schema / all schemas - depending on the database.
        If schema is empty, all schemas are searched.

        Returns: dict
        { ordinal_identifier: {
            'domain_schema': schema_name,
            'domain_name': domain_name,
            'source_domain_sql':
                - Original SQL statement to create the domain from the source database
                - Contains all the specific syntax of the source database
            'domain_data_type':
                - data type of the column /data type of the domain
            'source_domain_check_sql':
                - SQL statement to create the domain stript of all the specific syntax of the source database
                - Should contains only the check expression
                - This is used for creating corresponding object in the target database (in PostgreSQL it is additional CHECK constraint)
            'domain_comment': domain_comment
            }
        }
        """
        pass

    @abstractmethod
    def get_create_domain_sql(self, settings):
        """
        This function is currently relevant only for target database
        Centralizes creation of SQL DDL statement for domains
        settings:
            -
        """
        pass

    @abstractmethod
    def testing_select(self):
        """
        Simple select statement to test the connection - like "SELECT 1"
        Some databases require special form of statement
        """
        pass

    @abstractmethod
    def get_database_version(self):
        """
        Returns the version of the database.
        This is used for debugging purposes and for checking compatibility with the migrator.
        """
        pass

    @abstractmethod
    def get_database_size(self):
        """
        Returns the size of the database in bytes.
        This is used for debugging purposes and for checking compatibility with the migrator.
        """
        pass

    @abstractmethod
    def get_top10_biggest_tables(self, settings):
        """
        Settings - dictionary with the following keys
            - source_schema: str - schema name of the tables to be checked
        Returns the top 10 biggest tables in the database.
        This is used for debugging purposes and for checking compatibility with the migrator.
        Returns a dictionary: {ordinal_number: {table_name: table_size}} sorted by table_size in descending order.
        """
        pass

if __name__ == "__main__":
    print("This script is not meant to be run directly")
