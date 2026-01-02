#!/usr/bin/env python

import sys
import argparse
import code
import time
import logging

# we want Pandas and NumPy to be available at the command line
import pandas as pd
import numpy as np

from library_scripts import *
from spotify_scripts import *
from common_tasks import *

logger = logging.getLogger(__name__)

_should_quit = False

def _quit():
    global _should_quit
    _should_quit = True


def _python_shell(shell_locals):
    global _should_quit

    ic = code.InteractiveConsole(shell_locals)

    # This code is taken from code.InteractiveConsole.interact().
    # It is modified to have better behavior around errors

    ic.write('Welcome to djlibman! Type quit() to exit\n')
    cprt = 'Type "help", "copyright", "credits" or "license" for more information.'
    ic.write("Python %s on %s\n%s\n(%s)\n" %
               (sys.version, sys.platform, cprt,
                'InteractiveConsole'))

    ps1 = ">>> "
    ps2 = "... "

    more = False
    while not _should_quit:
        try:
            if more:
                prompt = ps2
            else:
                prompt = ps1
            try:
                # all of this complicated mess is required for the prompt to appear after
                # the syntax error (if there is one). This may be a side effect of the IDE...
                sys.stderr.flush()
                time.sleep(0.1)
                sys.stdout.write(prompt)
                sys.stdout.flush()
                line = ic.raw_input('')
            except EOFError:
                ic.write("\n")
                break
            else:
                more = ic.push(line)
        except KeyboardInterrupt:
            ic.write("\nKeyboardInterrupt\n")
            ic.resetbuffer()
            more = False

    ic.write('Exiting djlibman shell...\n')
    return


def _init(config_file=None):
    djlib_config.init(config_file)

    logging_args = {
        'level': djlib_config._log_level,
        'format': '%(asctime)s %(name)s:%(levelname)s: %(message)s',
        'force': True
    }
    if djlib_config._log_file is not None:
        logging_args['filename'] = djlib_config._log_file
    logging.basicConfig(**logging_args)

    # HACK; after basicConfig, we have to silence a few loggers that might log too much
    # TODO better way: set logging to INFO everywhere and allow individual packages to increase their logging
    if djlib_config._log_level <= logging.DEBUG:
        import spotipy
        spotipy.client.logger.setLevel(logging.INFO)
        import urllib3
        urllib3.connectionpool.log.setLevel(logging.INFO)

    logger.info('Logging started')
    logger.debug('Test debug message')

    return

def _main():
    parser = argparse.ArgumentParser(
        prog='djlibman.py',
        description='Organizes a DJ library between Rekordbox, Spotify, YouTube, google sheets and CSVs'
    )
    parser.add_argument('-c', '--config', default=None)

    args = parser.parse_args()

    config_file = args.config

    _init(config_file)

    # build the local vars of the console
    # this is the equivalent of 'import * from ...'

    shell_locals = {}
    shell_locals.update(globals())

    # drop into a shell
    _python_shell(shell_locals)

    return 0

if __name__ == '__main__':
    sys.exit(_main())
else:
    _init()

