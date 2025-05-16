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
    def fetch_table_columns(self, table_schema: str, table_name: str, migrator_tables) -> dict:
        """
        Returns a dictionary describing the schema of the specific table
        Items names and values correspond with INFORMATION_SCHEMA.COLUMNS table
        In case of legacy databases, content is suplied from system tables
        Columns starting with 'replaced_*' store substituted values
        Some connectors might add specific columns but these are not recognized by other connectors
        Not all columns are used in all connectors

        { column_ordinary_number: {
            'column_name': column_name,
            'is_nullable': 'YES' or 'NO',
            'column_default': column_default_value,
            'replaced_column_default': custom replacement value,
            'data_type': data type from table definition,
            'replaced_data_type': custom replacement value,
            'column_type': full description of data type from table definition with all parameters,
            'replaced_column_type': custom replacement value,
            'character_maximum_length': length of the column,
            'numeric_precision': precision of the column,
            'numeric_scale': scale of the column,
            'basic_data_type': basic data type for user defined types,
            'basic_column_type': basic column type for user defined types with all parameters,
            'is_identity': 'YES' or 'NO',
            'column_comment': comment for the column,
            'is_generated': 'YES' or 'NO',
            'generation_expression': expression for generated column,
            }
        }

        ## Special notes for some databases:
        # Informix default values: https://www.ibm.com/docs/en/informix-servers/12.10?topic=tables-sysdefaults
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
        """
        pass

    @abstractmethod
    def get_create_table_sql(self, settings):
        """
        This function is relevant only for target database
        Centralizes creation of SQL DDL statement
        settings - dictionary with the following keys
            - target_db_type: str - target database type
            - target_schema: str - schema name of the table in the target database
            - target_table_name: str - table name in the source database
            - source_columns: dict - dictionary of columns to be converted
            - converted_columns: dict - dictionary of converted columns
        Returns:
          - SQL statement to create the table in the target database - used for table creation
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
        settings - dictionary with the following keys
            - source_table_id: id of the table in the source database (does not exist in MySQL)
            - source_schema: schema name of the table in the source database
            - source_table_name: table name in the source database
            - target_schema: target schema name
            - target_table_name: target table name
            - target_columns: list of target columns
        Returned SQL for index creation must be compatible with the target database.
        But this is usually not a big problem, syntax is usually the same for most databases.
        Returns a dictionary:
            { ordinary_number: {
                'name': index_name,
                'type': index_type,   # INDEX, UNIQUE, PRIMARY KEY
                'owner': index_owner,  ## might be useful for some source databases
                'columns': "column_name1, column_name2, ..."
                'columns_count': index_columns_count,
                'columns_data_types': [data_type1, data_type2, ...],
                'sql': create_index_sql,
                'comment': index_comment
                }
            }
        Note to 'owner' value: some source databases like Informix have a concept of system indexes,
        which are automatically created by the database engine. For example missing primary key index
        on a table if Foreign Key constraint is defined on that column.
        In this case, the owner of the index is set to 'informix' and these indexes might be confusing
        for the user because they are not defined in his data model.
        """
        pass

    @abstractmethod
    def fetch_constraints(self, settings):
        """
        settings - dictionary with the following keys
            - source_table_id: id of the table in the source database (does not exist in MySQL)
            - source_schema: schema name of the table in the source database
            - source_table_name: table name in the source database
            - target_schema: target schema name
            - target_table_name: target table name
        Fetch constraints for a table.
        Returns a dictionary:
            { ordinary_number: {
                'name': constraint_name:
                'type': constraint_type,
                'sql': create_constraint_sql,
                'comment': constraint_comment
                }
            }
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
    def convert_funcproc_code(self, funcproc_code: str, target_db_type: str, source_schema: str, target_schema: str, table_list: list):
        """
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
    def testing_select(self):
        """
        Simple select statement to test the connection.
        Some databases require special form of statement
        """
        pass