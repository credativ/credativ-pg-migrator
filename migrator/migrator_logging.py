import logging
import inspect
import os
import signal
import sys

class MigratorLogger:
    def __init__(self, log_file):
        self.logger = logging.getLogger('migrator')
        self.logger.setLevel(logging.DEBUG)

        # Check if handlers are already added to avoid duplicate logs
        if not self.logger.handlers:
            # Create file handler which logs even debug messages
            fh = logging.FileHandler(log_file)
            fh.setLevel(logging.DEBUG)

            # Create console handler which logs even debug messages
            ch = logging.StreamHandler()
            ch.setLevel(logging.DEBUG)

            # Create formatter and add it to the handlers
            formatter = logging.Formatter('%(asctime)s: [%(levelname)s] %(message)s')
            fh.setFormatter(formatter)
            ch.setFormatter(formatter)

            # Add the handlers to the logger
            self.logger.addHandler(fh)
            self.logger.addHandler(ch)

    def stop_logging(self):
        for handler in self.logger.handlers[:]:
            handler.close()
            self.logger.removeHandler(handler)

if __name__ == "__main__":
    print("This script is not meant to be run directly")
