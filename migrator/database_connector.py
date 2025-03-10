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
            'table_name': table_name
            }
        }
        """
        pass

    @abstractmethod
    def fetch_table_columns(self, table_schema: str, table_name: str, migrator_tables) -> dict:
        """
        Returns a dictionary describing the schema of the specific table
        { column_ordinary_number: {
            'name': column_name,
            'type': column_type,
            'length': column_length,
            'nullable': column_nullable,
            'default': column_default_value,
            'other': specific_column_properties
            }
        }
        # Informix default values: https://www.ibm.com/docs/en/informix-servers/12.10?topic=tables-sysdefaults
        """
        pass

    @abstractmethod
    def convert_table_columns(self, target_db_type: str, table_schema: str, table_name: str, columns: dict):
        """
        Converts the columns of one source table to the target database type and SQL syntax.
        Returns:
          - dictionary of converted columns - the same as dictionaly returned by fetch_table_columns, but with all conversion -> necessary for data migration
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
    def fetch_indexes(self, table_id: int, table_schema: str, table_name: str):
        """
        Fetch indexes for a table.
        Returned SQL for index creation must be compatible with the target database.
        But this is usually not a big problem, syntax is usually the same for most databases.
        Returns a dictionary:
            { ordinary_number: {
                'name': index_name,
                'type': index_type,   # INDEX, UNIQUE, PRIMARY KEY
                'columns': "column_name1, column_name2, ..."
                'sql': create_index_sql,
                }
            }
        """
        pass

    @abstractmethod
    def fetch_constraints(self, table_id: int, table_schema: str, table_name: str):
        """
        Fetch constraints for a table.
        Returns a dictionary:
            { ordinary_number: {
                'name': constraint_name:
                'type': constraint_type,
                'sql': create_constraint_sql
                }
            }
        """
        pass

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
                'sql': create_trigger_sql
                }
            }
        """
        pass
    @abstractmethod
    def fetch_funcproc_names(self, schema: str):
        """
        Fetch function and procedure names in the specified schema.
        Returns: dict
        { ordinary_number: {
            'name': funcproc_name:
            'id': funcproc_id,
            'type': 'FUNCTION' or 'PROCEDURE'
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
