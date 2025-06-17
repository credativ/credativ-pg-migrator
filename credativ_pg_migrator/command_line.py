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

import argparse
from credativ_pg_migrator.constants import MigratorConstants

class CommandLine:
    def __init__(self):
        self.parser = argparse.ArgumentParser(description=f"""{MigratorConstants.get_full_name()}, version: {MigratorConstants.get_version()}""")
        self.args = None
        self.setup_arguments()

    def setup_arguments(self):
        self.parser.add_argument(
            '--log-level',
            default='INFO',
            choices=MigratorConstants.get_message_levels(),
            help="Set the logging level")

        self.parser.add_argument(
            '--dry-run',
            action='store_true',
            help="Run the tool in dry-run mode")

        self.parser.add_argument(
            '--config',
            type=str,
            help='Path/name of the configuration file')
            # required=True)

        self.parser.add_argument(
            '--log-file',
            type=str,
            default=MigratorConstants.get_default_log(),
            help=f'Path/name of the log file (default: {MigratorConstants.get_default_log()})')

        self.parser.add_argument(
            '--version',
            action='store_true',
            help='Show the version of the tool')

    def parse_arguments(self):
        self.args = self.parser.parse_args()
        # If --version is not set, check for required arguments manually
        if not self.args.version and not self.args.config:
            self.parser.error("--config is required")
        return self.args

    def print_all(self, logger):
        if self.args.log_level:
            logger.info("Commmand line parameters:")
            logger.info("log_level    = {}".format(self.args.log_level))
            logger.info("dry_run      = {}".format(self.args.dry_run))
            logger.info("config       = {}".format(self.args.config))
            logger.info("log          = {}".format(self.args.log_file))
            # logger.info("migrator_dir = {}".format(self.args.migrator_dir))

    def get_parameter_value(self, param_name):
        param_name = param_name.replace("-", "_")
        return getattr(self.args, param_name, None)

if __name__ == "__main__":
    print("This script is not meant to be run directly")
