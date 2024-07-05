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
    def __init__(self,
                 rekordbox_xml,
                 backups=0
                 ):
        self._path = rekordbox_xml
        self._backups = backups

        self._last_read_time = None
        self._xml = None
        self._collection = None
        self._playlists = None

        return

    def _refresh(self):
        if self._last_read_time is None or os.path.getmtime(self._path) > self._last_read_time:
            self._parse()

        return

    def _parse(self):
        self._last_read_time = os.path.getmtime(self._path)

        self._collection = None
        self._playlists = None

        self._xml = ET.parse(self._path)

        xml_root = self._xml.getroot()

        for child in xml_root:
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

    @classmethod
    def _reduce_playlist(cls, playlist):
        if isinstance(playlist, pd.Index):
            return len(playlist)
        if isinstance(playlist, dict):
            return { key: cls._reduce_playlist(value) for key, value in playlist.items() }

    def get_playlist_names(self):
        self._refresh()
        return RekordboxInterface._reduce_playlist(self._playlists)

    def get_playlist_track_ids(self, playlist_name):
        self._refresh()

        if isinstance(playlist_name, str):
            playlist_name = [playlist_name]

        playlist = self._playlists
        for i in range(len(playlist_name)):
            if not isinstance(playlist, dict):
                raise ValueError('Playlist %s is not a folder playlist' % playlist_name[:(i-1)])
            playlist = playlist[playlist_name[i]]

        if not isinstance(playlist, pd.Index):
            raise ValueError('Playlist %s is not a leaf playlist' % playlist_name)

        return pd.Index(playlist)

    def get_playlist_tracks(self, playlist_name):
        self._refresh()

        track_ids = self.get_playlist_track_ids(playlist_name)
        return self._collection.loc[track_ids]

    def write(self):
        if self._xml is None:
            raise Exception('Cannot write Rekordbox XML before reading')

        back_up_file(self._path, self._backups)

        self._xml.write(self._path)

        return


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

    df = pd.DataFrame.from_records(collection)
    df = infer_types(df)

    # has to be done after infer_types so that the TrackIDs in the index are ints and not strings
    df = df.set_index(df.TrackID)

    return df


def _parse_playlists(node: ET.Element):
    assert node.tag == 'PLAYLISTS'

    assert len(node) == 1
    child = node[0]

    assert child.tag == 'NODE'
    assert child.attrib['Name'] == 'ROOT'
    assert child.attrib['Type'] == '0'

    return _parse_playlist_node(child)

def _parse_playlist_node(node: ET.Element):
    assert node.tag == 'NODE'

    type = node.attrib['Type']
    if type == '0':
        # playlist folder
        playlists = {}
        for child in node:
            name = child.attrib['Name']
            assert name not in playlists

            playlists[name] = _parse_playlist_node(child)

        return playlists
    elif type == '1':
        # leaf playlist
        track_ids = []

        for child in node:
            assert child.tag == 'TRACK'
            track_id = np.int64(child.attrib['Key'])
            track_ids.append(track_id)

        return pd.Index(track_ids, name='TrackID')
    else:
        assert False


def _debug_print_xml_node(node, indent=0):
    print(" "*indent + "tag='%s' attrib=%s children=%d" % (node.tag, node.attrib, len(node)))
    # tag: string
    # attrib: dict
    for child in node:
        _debug_print_xml_node(child, indent + 4)
    return
