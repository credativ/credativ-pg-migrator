from setuptools import setup, find_packages

setup(
    name='credativ-pg-migrator',
    version='0.17.0dev',
    url='https://github.com/credativ/credativ-pg-migrator.git',
    author='Josef Machytka',
    author_email='josef.machytka@credativ.de',
    description='Migrator from proprietary and legacy databases into PostgreSQL',
    packages=find_packages(),
    package_data={'credativ_pg_migrator': ['config.schema.json']},
    include_package_data=True,
    install_requires=['psycopg2', 'jaydebeapi', 'pyyaml', 'pandas', 'pyodbc', 'tabulate', 'sqlglot', 'jsonschema'],
    entry_points={'console_scripts': ['credativ-pg-migrator = credativ_pg_migrator.main:main']},
)
