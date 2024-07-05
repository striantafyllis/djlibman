#!/usr/bin/env python

import sys
import argparse
import configparser
import os
import os.path
import readline
import code
import time
import ast

import file_interface
import google_interface
import rekordbox_interface
import spotify_interface

# we want Pandas and NumPy to be available at the command line
import pandas as pd
import numpy as np

# we want all utils and scripts to be available in the user python shell
from utils import *
from spotify_scripts import *
from local_scripts import *
from playlist_scripts import *

import spotify_scripts
import local_scripts
import playlist_scripts

def test_func():
    print('Test')
    return 'Test'

should_quit = False

def quit():
    global should_quit
    should_quit = True

class Context:
    def __init__(self):
        self.rekordbox = None
        self.google = None
        self.spotify = None
        self.docs = {}
        self.backups = 0
        return

    def set_backups(self, backups):
        self.backups = backups

    def set_rekordbox(self, **kwargs):
        if self.rekordbox is not None:
            raise Exception('Rekordbox already set')

        if 'rekordbox_xml' not in kwargs:
            raise Exception('Rekordbox: field rekordbox_xml has to be specified')
        else:
            rekordbox_xml = kwargs['rekordbox_xml']
            del kwargs['rekordbox_xml']

        if 'backups' not in kwargs:
            kwargs['backups'] = self.backups

        self.rekordbox = rekordbox_interface.RekordboxInterface(rekordbox_xml, **kwargs)

        return



    def add_doc(self, name, type, **kwargs):
        if name in self.docs:
            raise Exception("Duplicate doc name: '%s'" % name)

        if type == 'google_sheet':
            if self.google is None:
                raise Exception("Cannot add Google sheet '%s'; no Google connection specified" % name)

            doc = google_interface.GoogleSheet(self.google, **kwargs)
        else:
            if 'path' not in kwargs:
                raise Exception("Cannot add file document '%s'; no path specified" % name)
            path = kwargs['path']
            del kwargs['path']

            if 'backups' not in kwargs:
                kwargs['backups'] = self.backups

            if type == 'excel':
                doc = file_interface.ExcelSheet(path, **kwargs)
            elif type == 'csv':
                doc = file_interface.CsvFile(path, **kwargs)
            else:
                raise Exception("Unsupported doc type: '%s'" % type)

        self.docs[name] = doc
        return

    def delete_backups(self):
        for doc in self.docs.values():
            if isinstance(doc, file_interface.FileDoc):
                doc.delete_backups()
        return

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
    global context

    context = Context()
    local_scripts.context = context
    spotify_scripts.context = context
    playlist_scripts.context = context

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

    for section_name in config.sections():
        section = config[section_name]

        if section_name == 'general':
            for field in section.keys():
                if field == 'backups':
                    context.set_backups(section.getint('backups'))
                elif field.startswith('pandas.'):
                    pd.set_option(field[7:], section.getint(field))
                else:
                    raise Exception('Unknown field in config section %s: %s' % (section_name, field))

        elif section_name == 'rekordbox':
            kwargs = {}

            for field in section.keys():
                if field in ['rekordbox_xml']:
                    kwargs[field] = section[field]
                elif field in ['backups']:
                    kwargs[field] = section.getint(field)
                else:
                    raise Exception('Unknown field in config section %s: %s' % (section_name, field))

            context.set_rekordbox(**kwargs)

        elif section.name == 'google':
            context.google = google_interface.GoogleInterface(section)

        elif section.name == 'spotify':
            context.spotify = spotify_interface.SpotifyInterface(section)

        elif section_name.startswith('docs.'):
            name = section_name[5:]
            type = section['type']

            kwargs = {}

            for field in section.keys():
                if field in ['type']:
                    continue
                if field in ['path', 'index_column', 'sheet', 'datetime_format']:
                    kwargs[field] = section[field]
                elif field in ['header', 'backups']:
                    kwargs[field] = section.getint(field)
                elif field in ['list_columns', 'boolean_columns', 'datetime_columns']:
                    kwargs[field] = ast.literal_eval(section[field])
                else:
                    raise Exception("Unknown field in config section %s: %s" % (section_name, field))

            context.add_doc(name, type, **kwargs)

        else:
            raise Exception("Unrecognized config section: '%s'" % section_name)

    # build the local vars of the console

    # first add all the docs; in practice, only docs whose names are valid identifiers
    # will be accessible - the rest will be accessible through ctx itself

    shell_locals = dict(context.docs)

    # add google, rekordbox and spotify; note that any docs with
    # conflicting names will be overwritten
    # shell_locals['ctx'] = ctx
    shell_locals['rekordbox'] = context.rekordbox
    shell_locals['google'] = context.google
    shell_locals['spotify'] = context.spotify

    # finally, add all the globals; this gives us all the functions etc.
    # notice again that any conflicting document names will be overwritten
    shell_locals.update(globals())

    # drop into a shell
    python_shell(shell_locals)

    return 0

if __name__ == '__main__':
    sys.exit(main())
