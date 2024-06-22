#!/usr/bin/env python

import sys
import argparse
import configparser
import os
import os.path
import readline
import code
import time

import google_interface
import spotify_interface

import pandas as pd

def test_func():
    print('Test')
    return 'Test'

should_quit = False

def quit():
    global should_quit
    should_quit = True

class Context:
    def __init__(self):
        self.google = None
        self.spotify = None
        self.rekordbox = None
        self.docs = {}
        return

    def add_google_sheet(self, names, id, page, has_header=False, column_types = {}):
        if self.google is None:
            raise Exception("Cannot add Google sheet '%s'; no Google connection specified" % names[0])

        doc = google_interface.GoogleSheet(self.google, id, page, has_header, column_types)

        for name in names:
            if name in self.docs:
                raise Exception("Duplicate doc name: '%s'" % name)
            self.docs[name] = doc

        return

def get_column_types(config):
    column_types = {}

    for key in config:
        if key.startswith('type.'):
            column_name = key[5:]

            type_name = config[key]

            if type_name.upper() in ['INT', 'INTEGER']:
                type = int
            elif type_name.upper() in ['FLOAT', 'DOUBLE']:
                type = float
            elif type_name.upper() in ['BOOL', 'BOOLEAN']:
                type = bool
            elif type_name.upper() == 'LIST':
                type = list
            elif type_name.upper() in ['DATE', 'TIMESTAMP']:
                type = pd.Timestamp
            else:
                raise Exception("Unrecognized type name '%s'" % type_name)

            if column_name in column_types:
                raise Exception("Duplicate column type: '%s'" % column_name)

            column_types[column_name] = type

    return column_types


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

    if config_file is None:
        if os.path.exists('./config'):
            config_file = './config'
        elif os.path.exists(os.path.join(os.environ['HOME'], 'config')):
            config_file = os.path.join(os.environ['HOME'], 'config')
        else:
            raise Exception('Config file not found')
    elif not os.path.exists(config_file):
        raise Exception("Config file '%s' not found" % config_file)

    config = configparser.ConfigParser()
    config.read(config_file)

    ctx = Context()

    if 'google' in config.sections():
        ctx.google = google_interface.GoogleInterface(config['google'])

    if 'spotify' in config.sections():
        ctx.spotify = spotify_interface.SpotifyInterface(config['spotify'])

    for section_name in config.sections():
        if section_name in ['google', 'spotify']:
            continue
        elif section_name.startswith('docs.'):
            section = config[section_name]
            names = [section_name[5:]]
            if 'name' in section:
                names.append(section['name'])

            type = section['type']

            if type == 'google_sheet':
                id = section['id']
                page = section['page']
                has_header = section.getboolean('header')
                column_types = get_column_types(section)

                ctx.add_google_sheet(names, id, page, has_header, column_types)
            else:
                raise Exception("Unsupported doc type: '%s'" % type)

        else:
            raise Exception("Unrecognized section: '%s'" % section_name)

    # build the local vars of the console

    # first add all the docs; in practice, only docs whose names are valid identifiers
    # will be accessible - the rest will be accessible through ctx itself

    shell_locals = dict(ctx.docs)

    # add ctx itself, plus google, rekordbox and spotify; note that any docs with
    # conflicting names will be overwritten
    shell_locals['ctx'] = ctx
    shell_locals['google'] = ctx.google
    # TODO add spotify, rekordbox etc.

    # finally, add all the globals; this gives us all the functions etc.
    # notice again that any conflicting document names will be overwritten
    shell_locals.update(globals())

    # drop into a shell
    python_shell(shell_locals)

    return 0

if __name__ == '__main__':
    sys.exit(main())
