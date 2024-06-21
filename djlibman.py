#!/usr/bin/env python

import sys
import argparse
import configparser
import os
import os.path
import code

import google_interface
import spotify_interface

class Context:
    def __init__(self):
        self.google = None
        self.spotify = None
        self.rekordbox = None
        self.docs = {}
        return

    def add_google_sheet(self, names, id, page, has_header=False):
        if self.google is None:
            raise Exception("Cannot add Google sheet '%s'; no Google connection specified" % names[0])

        doc = google_interface.GoogleSheet(self.google, id, page, has_header)

        for name in names:
            if name in self.docs:
                raise Exception("Duplicate doc name: '%s'" % name)
            self.docs[name] = doc

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

                ctx.add_google_sheet(names, id, page, has_header)
            else:
                raise Exception("Unsupported doc type: '%s'" % type)

        else:
            raise Exception("Unrecognized section: '%s'" % section_name)

    code.interact(
        banner='Welcome to djlibman!',
        local=locals()
    )

    return 0

if __name__ == '__main__':
    sys.exit(main())


