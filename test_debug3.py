import yaml
with open("/home/josef/github.com/credativ/credativ-pg-migrator-dev/tests/postgresql_anonymization/omdb_anonymization.yaml") as f:
    config = yaml.safe_load(f)

print("Target keys:", config.keys())
print("Target block:", config.get('target'))
