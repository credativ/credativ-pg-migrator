"""
Migrator code
source venv/bin/activate
"""
from command_line import CommandLine
from config_parser import ConfigParser
from orchestrator import Orchestrator
from migrator_logging import MigratorLogger
from planner import Planner
import constants
import sys
import os
import traceback
import signal
import sys

def main():
    cmd = CommandLine()
    args = cmd.parse_arguments()

    # Check if the version flag is set
    if args.version:
        print(f"Version: {constants.MIGRATOR_VERSION}")
        sys.exit(0)

    signal.signal(signal.SIGINT, ctrlc_signal_handler)

    # Delete the log file if it exists
    if os.path.exists(args.log_file):
        os.remove(args.log_file)

    logger = MigratorLogger(args.log_file)

    try:
        logger.logger.info("Database Migration Tool")

        cmd.print_all(logger.logger)

        logger.logger.info('Starting configuration parser...')
        config_parser = ConfigParser(args)

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