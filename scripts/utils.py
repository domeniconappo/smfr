import os
import configparser
from itertools import chain


def import_env(env_file):
    parser = configparser.ConfigParser()
    with open(env_file) as lines:
        lines = chain(("[root]",), lines)  # make it a proper properties file for ConfigParser
        parser.read_file(lines)
    for option in parser.options('root'):
        os.environ[option.upper()] = parser.get('root', option)
