# credativ-pg-migrator

Repository contains python solution for migrations of whole database from legacy systems to PostgreSQL.

The tool is designed to migrate data from legacy systems to PostgreSQL databases. The tool uses a configuration file to define the source and destination databases, tables, and columns. The tool also supports logging and error handling.

## Testing run

python3 main.py --config=config.yaml --log-level=DEBUG
