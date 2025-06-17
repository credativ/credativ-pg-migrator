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

"""
Migrator code
source venv/bin/activate
"""
from credativ_pg_migrator.command_line import CommandLine
from credativ_pg_migrator.config_parser import ConfigParser
from credativ_pg_migrator.orchestrator import Orchestrator
from credativ_pg_migrator.migrator_logging import MigratorLogger
from credativ_pg_migrator.planner import Planner
from credativ_pg_migrator.constants import MigratorConstants
import sys
import os
import traceback
import signal

def main():
    cmd = CommandLine()
    args = cmd.parse_arguments()

    # Check if the version flag is set
    if args.version:
        print(f"{MigratorConstants.get_full_name()}")
        print(f"Version: {MigratorConstants.get_version()}")
        sys.exit(0)

    signal.signal(signal.SIGINT, ctrlc_signal_handler)

    # Delete the log file if it exists
    if os.path.exists(args.log_file):
        os.remove(args.log_file)

    logger = MigratorLogger(args.log_file)

    try:
        logger.logger.info(f"""{MigratorConstants.get_full_name()}, version: {MigratorConstants.get_version()}""")

        cmd.print_all(logger.logger)

        logger.logger.info('Starting configuration parser...')
        config_parser = ConfigParser(args, logger.logger)

        # Print the parsed configuration
        if args.log_level == 'DEBUG':
            logger.logger.debug(f"Parsed configuration: {config_parser.config}")

        logger.logger.info('Starting planner...')
        planner = Planner(config_parser)
        planner.create_plan()

        logger.logger.info('Starting orchestrator...')
        orchestrator = Orchestrator(config_parser)
        orchestrator.run()

        logger.logger.info("Migration Done")

    except Exception as e:
        logger.logger.error(f"An error in the main: {e}")
        sys.exit(1)

    finally:
        logger.stop_logging()
        exit()

def ctrlc_signal_handler(sig, frame):
    print("Program interrupted with Ctrl+C")
    traceback.print_stack(frame)
    sys.exit(0)

if __name__ == "__main__":
    main()
    print('All done')
