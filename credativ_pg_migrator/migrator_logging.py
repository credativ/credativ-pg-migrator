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

import logging

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
