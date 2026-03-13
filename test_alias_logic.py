import sys
import yaml
from credativ_pg_migrator.config_parser import ConfigParser

cp = ConfigParser('config_sample.yaml')
print(f"Use aliases: {cp.get_use_aliases_as_target_tables()}")
sys.exit(0)
