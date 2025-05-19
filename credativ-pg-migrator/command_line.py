import argparse
import constants

class CommandLine:
    def __init__(self):
        self.parser = argparse.ArgumentParser(description="""Data Migration Tool "credativ-pg-migrator", version: {}""".format(constants.MIGRATOR_VERSION))
        self.args = None
        self.setup_arguments()

    def setup_arguments(self):
        self.parser.add_argument(
            '--log-level',
            default='INFO',
            choices=['INFO', 'DEBUG'],
            help="Set the logging level")

        self.parser.add_argument(
            '--dry-run',
            action='store_true',
            help="Run the tool in dry-run mode")

        self.parser.add_argument(
            '--config',
            type=str,
            help='Path/name of the configuration file',
            required=True)

        self.parser.add_argument(
            '--log-file',
            type=str,
            default=constants.MIGRATOR_DEFAULT_LOG,
            help=f'Path/name of the log file (default: {constants.MIGRATOR_DEFAULT_LOG})')

        self.parser.add_argument(
            '--version',
            action='store_true',
            help='Show the version of the tool')

    def parse_arguments(self):
        self.args = self.parser.parse_args()
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
