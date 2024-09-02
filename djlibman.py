#!/usr/bin/env python

import sys
import argparse
import code
import time

import djlib_config

# we want Pandas and NumPy to be available at the command line
import pandas as pd
import numpy as np

def test_func():
    print('Test')
    return 'Test'

should_quit = False

def quit():
    global should_quit
    should_quit = True


def python_shell(shell_locals):
    global should_quit

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
    while not should_quit:
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


def main():
    parser = argparse.ArgumentParser(
        prog='djlibman.py',
        description='Organizes a DJ library between Rekordbox, Spotify, YouTube, google sheets and CSVs'
    )
    parser.add_argument('-c', '--config', default=None)

    args = parser.parse_args()

    config_file = args.config

    djlib_config.init(config_file)

    # build the local vars of the console
    # this is the equivalent of 'import * from ...'

    shell_locals = {}
    shell_locals.update(djlib_config.__dict__)

    import scripts
    shell_locals.update(scripts.__dict__)

    # finally, add all the globals; this gives us all the functions etc.
    # by doing this last, any conflicting document names will be overridden
    shell_locals.update(globals())

    # drop into a shell
    python_shell(shell_locals)

    return 0

if __name__ == '__main__':
    sys.exit(main())
