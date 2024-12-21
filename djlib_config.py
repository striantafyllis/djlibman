
import os
import os.path
import configparser
import ast
import logging

import pandas as pd

import rekordbox_interface
import spotify_interface
import google_interface
import file_interface

rekordbox = None
google = None
spotify = None
docs = {}
_backups = 0

_log_file = './djlibman.log'
_log_level = logging.INFO

def init(config_file=None):
    global rekordbox
    global google
    global spotify
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
                if field == 'backups':
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
                rekordbox_backups = _backups

            for field in section.keys():
                if field not in ['rekordbox_xml', 'backups']:
                    raise Exception('Unknown field in config section %s: %s' % (section_name, field))

            rekordbox = rekordbox_interface.RekordboxInterface(rekordbox_xml, rekordbox_backups)

        elif section.name == 'google':
            google = google_interface.GoogleInterface(section)

        elif section.name == 'spotify':
            spotify = spotify_interface.SpotifyInterface(section)

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

    if type == 'google_sheet':
        if google is None:
            raise Exception("Cannot add Google sheet '%s'; no Google connection specified" % name)

        doc = google_interface.GoogleSheet(google, **kwargs)
    else:
        if 'path' not in kwargs:
            raise Exception("Cannot add file document '%s'; no path specified" % name)
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

    docs[name] = doc
    if name not in globals():
        globals()[name] = doc

    return


def delete_backups():
    global docs

    for doc in docs.values():
        if isinstance(doc, file_interface.FileDoc):
            doc.delete_backups()
    return


