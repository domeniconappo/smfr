import argparse
import os
import sys
import configparser
from itertools import chain


class ParserHelpOnError(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write('error: %s\n' % message)
        self.print_help()
        sys.exit(2)


def import_env(env_file):
    parser = configparser.ConfigParser()
    with open(env_file) as lines:
        lines = chain(("[root]",), lines)  # This line does the trick.
        parser.read_file(lines)
    for option in parser.options('root'):
        os.environ[option.upper()] = parser.get('root', option)
