import yaml
with open("/home/josef/github.com/credativ/credativ-pg-migrator-dev/tests/postgresql_anonymization/omdb_anonymization.yaml") as f:
    config = yaml.safe_load(f)

print("Source connectivity:", config['source'].get('connectivity', None))
print("Target connectivity:", config['target'].get('connectivity', None))
print("Source config block:", config['source'])
print("Target config block:", config['target'])
