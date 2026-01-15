#!/usr/bin/env python

import xml.etree.ElementTree as ET
import os.path
import sys

import pandas as pd

from local_util import *

# Rename some Rekordbox attributes
_attrib_rename = {
    'TrackID': 'rekordbox_id',
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
        assert False

    @classmethod
    def _get_playlists(cls, playlist_container, parent_playlist):
        if isinstance(playlist_container, pd.Index):
            return [parent_playlist]
        if isinstance(playlist_container, dict):
            playlists = []
            for key, value in playlist_container.items():
                playlists += cls._get_playlists(value, parent_playlist + [key])
            return playlists
        assert False

    def get_playlists(self):
        self._refresh()
        return RekordboxInterface._get_playlists(self._playlists, [])

    def get_playlists_as_map(self):
        self._refresh()
        return RekordboxInterface._reduce_playlist(self._playlists)

    def pretty_print_playlists(self):
        pretty_print(self.get_playlists_as_map())

    def get_playlist_track_ids(self, playlist_name):
        self._refresh()

        if isinstance(playlist_name, str):
            playlist_name = [playlist_name]

        playlist = self._playlists
        for i in range(len(playlist_name)):
            if not isinstance(playlist, dict):
                raise ValueError('Playlist {playlist_name[:i]} is not a folder playlist')
            playlist = playlist.get(playlist_name[i])
            if playlist is None:
                return None

        if not isinstance(playlist, pd.Index):
            raise ValueError('Playlist %s is not a leaf playlist' % playlist_name)

        return pd.Index(playlist)

    def playlist_exists(self, playlist_name):
        return self.get_playlist_track_ids(playlist_name) is not None

    def get_playlist_tracks(self, playlist_name):
        self._refresh()

        track_ids = self.get_playlist_track_ids(playlist_name)
        return self._collection.loc[track_ids]

    def create_playlist(self, playlist_name, track_ids=[], overwrite=False):
        """Creates a new leaf playlist with the specified track IDs.
           If any of the containing folder playlists don't exist, they will be created.
           To create an empty folder playlist, set the last playlist name to None and the track IDs to empty"""
        self._refresh()

        if len(playlist_name) == 0:
            raise Exception('Cannot create the top-level folder')

        if playlist_name[-1] is None and len(track_ids) != 0:
            raise Exception('The playlist is an empty folder playlist but track_ids specified')

        if isinstance(track_ids, pd.DataFrame):
            track_ids = track_ids.rekordbox_id

        # make sure the track IDs exist
        if not isinstance(track_ids, pd.Index):
            track_ids = pd.Index(track_ids, name='rekordbox_id')

        unknown_track_ids = track_ids.difference(self._collection.index, sort=False)
        if len(unknown_track_ids) > 0:
            raise Exception('Unknown track IDs: %s' % unknown_track_ids.to_list())

        if isinstance(playlist_name, str):
            playlist_name = [playlist_name]

        xml_root = self._xml.getroot()

        containing_folder = None
        for child in xml_root:
            if child.tag == 'PLAYLISTS':
                containing_folder = child[0]
                break
        if containing_folder is None:
            raise Exception('rekordbox.xml: no PLAYLISTS node')

        assert containing_folder.attrib['Name'] == 'ROOT'

        for i in range(len(playlist_name)-1):
            if containing_folder.attrib['Type'] != '0':
                raise Exception('Playlist %s is not a folder playlist' % playlist_name[:i])
            next_containing_folder = None

            for child in containing_folder:
                if child.attrib['Name'] == playlist_name[i]:
                    next_containing_folder = child
                    break

            if next_containing_folder is None:
                print('Creating folder playlist %s' % playlist_name[:(i+1)])

                next_containing_folder = ET.Element('NODE', attrib = {
                    'Name': playlist_name[i],
                    'Type': '0',
                    'Count': '0'
                })
                containing_folder.append(next_containing_folder)
                containing_folder.attrib['Count'] = str(int(containing_folder.attrib['Count'])+1)

            containing_folder = next_containing_folder

        name = playlist_name[-1]

        if name is None:
            # empty folder playlist case
            return

        for i in range(len(containing_folder)):
            child = containing_folder[i]

            if child.attrib['Name'] == name:
                if not overwrite:
                    raise Exception('Playlist %s already exists' % playlist_name)
                if child.attrib['Type'] != '1':
                    raise Exception('Playlist %s already exists and is not a leaf playlist' % playlist_name)

                containing_folder.remove(child)
                insert_index = i
                break

        insert_index = len(containing_folder)
        containing_folder.attrib['Count'] = str(len(containing_folder)+1)

        playlist = ET.Element('NODE', attrib = {
            'Name': name,
            'Type': '1',
            'KeyType': '0',
            'Entries': str(len(track_ids))
        })

        for track_id in track_ids:
            track = ET.Element('TRACK', attrib = {
                'Key': str(track_id)
            })
            playlist.append(track)

        containing_folder.insert(insert_index, playlist)

        return

    def delete_playlist(self, playlist_name, recursive=False):
        """Deletes a playlist. If recursive=False, trying to delete a non-empty folder will cause an exception."""
        self._refresh()

        if len(playlist_name) == 0:
            raise Exception('Cannot create the top-level folder')

        if isinstance(playlist_name, str):
            playlist_name = [playlist_name]

        xml_root = self._xml.getroot()

        playlist = None
        for child in xml_root:
            if child.tag == 'PLAYLISTS':
                playlist = child[0]
                break
        if playlist is None:
            raise Exception('rekordbox.xml: no PLAYLISTS node')

        assert playlist.attrib['Name'] == 'ROOT'

        for i in range(len(playlist_name)):
            if playlist.attrib['Type'] != '0':
                raise Exception('Playlist %s is not a folder playlist' % playlist_name[:i])
            next_playlist = None

            for child in playlist:
                if child.attrib['Name'] == playlist_name[i]:
                    next_playlist = child
                    break

            if next_playlist is None:
                raise Exception('Playlist %s does not exist' % playlist_name[:(i+1)])

            containing_playlist = playlist
            playlist = next_playlist

        if not recursive and playlist.attrib['Type'] == '0' and len(playlist) > 0:
            raise Exception('Playlist %s is a non-empty folder' % playlist_name)

        containing_playlist.remove(playlist)
        containing_playlist.attrib['Count'] = str(int(containing_playlist.attrib['Count'])-1)

        return


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

    # has to be done after infer_types so that the track IDs in the index are ints and not strings
    df = df.set_index(df.rekordbox_id)

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

        return pd.Index(track_ids, name='rekordbox_id')
    else:
        assert False


def _debug_print_xml_node(node, indent=0):
    print(" "*indent + "tag='%s' attrib=%s children=%d" % (node.tag, node.attrib, len(node)))
    # tag: string
    # attrib: dict
    for child in node:
        _debug_print_xml_node(child, indent + 4)
    return
