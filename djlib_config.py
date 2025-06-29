
import os
import os.path
import configparser
import ast
import logging
import re

import pandas as pd

import rekordbox_interface
import spotify_interface
import google_interface
import file_interface

default_dir = None

rekordbox = None
google = None
spotify = None
docs = {}
_backups = 0
spotify_queues = []
discography_cache_dir = None
artist_albums_ttl_days = 30

# TODO make these configurable
fuzzy_match_cutoff_threshold = 0.6
fuzzy_match_automatic_accept_threshold = 0.9

_log_file = './djlibman.log'
_log_level = logging.INFO

def init(config_file=None):
    global default_dir
    global rekordbox
    global google
    global spotify
    global spotify_queues
    global discography_cache_dir
    global artist_albums_ttl_days
    global discography_verbose
    global docs
    global _backups
    global _log_file
    global _log_level

    if config_file is None:
        potential_config_files = [
            os.path.join(os.environ['HOME'], '.djlib', 'config'),
            os.path.join(os.environ['HOME'], '.djlib_config'),
            './config'
        ]

        for potential_config_file in potential_config_files:
            if os.path.exists(potential_config_file):
                config_file = potential_config_file
                break

        if config_file is None:
            raise Exception('Config file not found')

    config = configparser.ConfigParser()
    config.read(config_file)

    for section_name in config.sections():
        section = config[section_name]

        if section_name == 'general':
            for field in section.keys():
                if field == 'default_dir':
                    default_dir = section[field]
                elif field == 'backups':
                    _backups = section.getint(field)
                elif field == 'logfile':
                    _log_file = section.get(field)
                    if _log_file.upper() == 'CONSOLE':
                        _log_file = None
                elif field == 'loglevel':
                    _log_level = getattr(logging, section.get(field).upper())
                elif field.startswith('pandas.'):
                    pd.set_option(field[7:], section.getint(field))
                else:
                    raise Exception('Unknown field in config section %s: %s' % (section_name, field))

        elif section_name == 'rekordbox':
            rekordbox_xml = section['rekordbox_xml']
            if 'backups' in section:
                rekordbox_backups = section.getint('backups')
            else:
                rekordbox_backups = 0

            for field in section.keys():
                if field not in ['rekordbox_xml', 'backups']:
                    raise Exception('Unknown field in config section %s: %s' % (section_name, field))

            rekordbox = rekordbox_interface.RekordboxInterface(rekordbox_xml, rekordbox_backups)

        elif section.name == 'google':
            google = google_interface.GoogleInterface(section)

        elif section.name == 'spotify':
            spotify = spotify_interface.SpotifyInterface(section)

        elif section.name == 'spotify_queues':
            for field in section.keys():
                m = re.match(r'level([1-9][0-9]*)', field)
                if m is None:
                    raise ValueError(f"Config section [spotify_queues]: bad field name '{field}'")

                level = int(m.group(1))

                if len(spotify_queues) + 1 != level:
                    raise ValueError(f"Config section [spotify_queues]: level "
                                     f"{level} encountered after level {len(spotify_queues)}")

                value = section.get(field)

                if value.startswith('[') or value.startswith("'"):
                    value = ast.literal_eval(value)

                if not isinstance(value, list):
                    value = [value]

                for v in value:
                    if not isinstance(v, str):
                        raise ValueError(f"Config section [spotify_queues]: invalid value type "
                                         f"{type(v)}")

                spotify_queues.append(value)

        elif section_name == 'spotify_discography':
            for field in section.keys():
                if field == 'discography_cache_dir':
                    discography_cache_dir = section[field]
                elif field == 'artist_albums_ttl_days':
                    artist_albums_ttl_days = section.getint(field)
                elif field == 'discography_verbose':
                    discography_verbose = section.getint(field)
                else:
                    raise Exception(f'Unknown field in config section {section_name}: {field}')

            if discography_cache_dir is None:
                discography_cache_dir = default_dir + '/discography_cache'

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

            _add_doc(name, type, **kwargs)

        else:
            raise Exception("Unrecognized config section: '%s'" % section_name)

def _add_doc(name, type, **kwargs):
    global google
    global docs
    global _backups

    if name in docs:
        raise Exception("Duplicate doc name: '%s'" % name)

    doc = create_doc(name, type, **kwargs)

    docs[name] = doc
    if name not in globals():
        globals()[name] = doc

    return

def create_doc(name, type='csv', **kwargs):
    if type == 'google_sheet':
        if google is None:
            raise Exception("Cannot add Google sheet '%s'; no Google connection specified" % name)

        doc = google_interface.GoogleSheet(google, **kwargs)
    else:
        if default_dir is None:
            raise Exception(f'Doc {name}: no default dir and no path specified')

        if 'path' not in kwargs:
            if type == 'csv':
                extension = '.csv'
            elif type == 'excel':
                extension = '.xlsx'
            else:
                raise Exception("Unsupported doc type: '%s'" % type)

            path = os.path.join(default_dir, name)
            if not name.endswith(extension):
                path += extension

        else:
            path = kwargs['path']
            del kwargs['path']

        if 'backups' not in kwargs:
            kwargs['backups'] = _backups

        if type == 'excel':
            doc = file_interface.ExcelSheet(path, **kwargs)
        elif type == 'csv':
            doc = file_interface.CsvFile(path, **kwargs)
        else:
            raise Exception("Unsupported doc type: '%s'" % type)

    return doc



def delete_backups():
    global docs

    for doc in docs.values():
        if isinstance(doc, file_interface.FileDoc):
            doc.delete_backups()
    return

def all_spotify_queues():
    all_queues = []
    for level in spotify_queues:
        all_queues += level

    return all_queues

def get_spotify_queue_level(queue_name):
    for i, level in enumerate(spotify_queues):
        if queue_name in level:
            return i+1

    return None

def get_default_spotify_queue_at_level(level):
    if level <= 0:
        raise ValueError(f'Invalid Spotify queue level {level}')
    if level > len(spotify_queues):
        raise ValueError(f"Spotify queue level {level} does not exist")

    return spotify_queues[level-1][0]
