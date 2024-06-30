#!/usr/bin/env python

import xml.etree.ElementTree as ET
import os.path
import sys

import pandas as pd

from internal_utils import *

# Rename some Rekordbox attributes
_attrib_rename = {
    'Artist': 'Artists',
    'Name': 'Title',
    'AverageBpm': 'BPM',
    'Tonality': 'Key',
    'DateAdded': 'Date Added',
    'TotalTime': 'Duration'
}

class RekordboxInterface:
    def __init__(self, config):
        self._rekordbox_xml = config['rekordbox_xml']
        self._playlist_dir = config['playlist_dir']

        for field in config.keys():
            if field not in ['rekordbox_xml', 'playlist_dir']:
                raise Exception('Unknown field in config section rekordbox: %s' % field)

        self._last_read_time = None
        self._collection = None
        self._playlists = None

        return

    def _refresh(self):
        if self._last_read_time is None or os.path.getmtime(self._rekordbox_xml) > self._last_read_time:
            self._parse()

        return

    def _parse(self):
        self._last_read_time = os.path.getmtime(self._rekordbox_xml)

        self._collection = None
        self._playlists = None

        library = ET.parse(self._rekordbox_xml)

        library_root = library.getroot()

        for child in library_root:
            if child.tag == 'PRODUCT':
                continue
            elif child.tag == 'COLLECTION':
                assert self._collection is None
                self._collection = _parse_collection(child)
            elif child.tag == 'PLAYLISTS':
                assert self._playlists is None
                assert self._collection is not None
                self._playlists = _parse_playlists(child)
            else:
                sys.stderr.write('WARNING: Unprocessed child: COLLECTION -> %s\n' % child.tag)

        assert self._collection is not None
        assert self._playlists is not None

        return

    def get_collection(self):
        self._refresh()
        return self._collection

    def get_playlist_names(self):
        self._refresh()
        return list(self._playlists.keys())

    def get_playlist_track_ids(self, playlist_name):
        self._refresh()

        return pd.Series(self._playlists[playlist_name])

    def get_playlist_tracks(self, playlist_name):
        self._refresh()

        return self._collection.loc[self._playlists[playlist_name]]


def _parse_collection(node: ET.Element):
    assert node.tag == 'COLLECTION'
    collection = []

    for child in node:
        if child.tag == 'TRACK':
            track = {
                _attrib_rename.get(key, key): value
                for key, value in child.attrib.items()
            }
            collection.append(track)
        else:
            raise Exception('Unknown tag %s in node COLLECTION' % child.tag)

    collection_columnar = list_of_dicts_to_dict_of_lists(collection)

    collection_columnar_typed = {
        key: infer_type(value)
        for key, value in collection_columnar.items()
    }

    return pd.DataFrame(collection_columnar_typed, index=collection_columnar_typed['TrackID'])


def _parse_playlists(node: ET.Element):
    assert node.tag == 'PLAYLISTS'

    playlists = {}

    for child in node:
        playlists.update(_parse_playlist_node(child, []))

    return playlists

def _parse_playlist_node(node: ET.Element,
                         prefix: list[str]):
    assert node.tag == 'NODE'

    name = node.attrib['Name']
    type = node.attrib['Type']
    if type == '0':
        # playlist container
        playlists = {}
        if name != 'ROOT':
            prefix = prefix + [name]
        for child in node:
            playlists.update(_parse_playlist_node(child, prefix))

        return playlists
    elif type == '1':
        # leaf playlist
        full_name = '/'.join(prefix + [name])

        track_ids = []

        for child in node:
            assert child.tag == 'TRACK'
            track_id = np.int64(child.attrib['Key'])
            track_ids.append(track_id)

        return { full_name: track_ids }


def _debug_print_xml_node(node, indent=0):
    print(" "*indent + "tag='%s' attrib=%s children=%d" % (node.tag, node.attrib, len(node)))
    # tag: string
    # attrib: dict
    for child in node:
        _debug_print_xml_node(child, indent + 4)
    return
