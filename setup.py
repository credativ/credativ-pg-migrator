from setuptools import setup, find_packages

setup(
    name='credativ-pg-migrator-dev',
    version='0.0',
    url='https://github.com/credativ/credativ-pg-migrator-dev.git',
    author='Josef Machytka',
    author_email='josef.machytka@credativ.de',
    description='Description of my package',
    packages=find_packages(),
    install_requires=['psycopg2', 'jaydebeapi', 'pyyaml', 'pandas', 'pyodbc'],
    entry_points={'console_scripts': ['credativ-pg-migrator = migrator:main']},
)
