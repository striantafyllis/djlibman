#!/usr/bin/env python

import xml.etree.ElementTree as ET
import urllib.parse
import re

from data_model import *
from utils import *


default_playlist_dir = '/Users/spyros/Music/'
default_rekordbox_xml = default_playlist_dir + 'rekordbox.xml'

# Rename some Rekordbox attributes that are obscurely named so that they agree with the Google sheet
_attrib_rename = {
    'TrackID': 'Rekordbox ID',
    'Artist': 'Artists',
    'Name': 'Title',
    'AverageBpm': 'BPM',
    'Tonality': 'Key',
    'DateAdded': 'Date Added',
    'TotalTime': 'Duration'
}


def _parse_collection(node: ET.Element):
    assert node.tag ==  'COLLECTION'
    collection = Library('Rekordbox')

    for child in node:
        if child.tag == 'TRACK':
            track = _parse_collection_track(child)
            collection.append(track)
        else:
            raise Exception('Unknown tag %s in node COLLECTION' % child.tag)

    return collection

def _parse_collection_track(node: ET.Element):
    assert node.tag == 'TRACK'

    attributes = {
        _attrib_rename.get(key, key): infer_type(value)
        for key, value in node.attrib.items()
    }

    id = attributes['Rekordbox ID']
    artists = frozenset(re.split(r' *[,&] *', attributes['Artists']))
    title = attributes['Title']

    return Track(id, artists, title, attributes)


def _parse_playlists(node: ET.Element, collection: Library):
    assert node.tag == 'PLAYLISTS'

    all_playlists = []

    for child in node:
        all_playlists += _parse_playlist_node(child, collection, [])

    playlists = {}

    for playlist in all_playlists:
        assert playlist.name not in playlists
        playlists[playlist.name] = playlist

    return playlists

def _parse_playlist_node(node: ET.Element,
                         collection: Library,
                         prefix: list[str]):
    assert node.tag == 'NODE'

    name = node.attrib['Name']
    type = node.attrib['Type']
    if type == '0':
        # playlist container
        all_playlists = []
        if name != 'ROOT':
            prefix = prefix + [name]
        for child in node:
            all_playlists += _parse_playlist_node(child, collection, prefix)

        return all_playlists
    elif type == '1':
        # leaf playlist
        full_name = '/'.join(prefix + [name])

        # Rekordbox playlists have no ID, but their names are unique, so I'll just
        # use the name as the ID also
        playlist = Playlist(full_name, full_name)

        for child in node:
            assert child.tag == 'TRACK'
            track_id = int(child.attrib['Key'])

            track = collection.get_track_by_id(track_id)
            if track is None:
                raise Exception('Unknown track ID %d in playlist %s' % (track_id, full_name))

            playlist.append(track)

        return [playlist]


def _debug_print_xml_node(node, indent=0):
    print(" "*indent + "tag='%s' attrib=%s children=%d" % (node.tag, node.attrib, len(node)))
    # tag: string
    # attrib: dict
    for child in node:
        _debug_print_xml_node(child, indent + 4)
    return


def parse_library(library_file=default_rekordbox_xml) -> tuple[Library, list[Playlist]]:
    library = ET.parse(library_file)

    library_root = library.getroot()
    # debug_print_xml_node(library_root)

    collection = None
    playlists = None

    for child in library_root:
        if child.tag == 'PRODUCT':
            continue
        elif child.tag == 'COLLECTION':
            assert collection is None
            collection = _parse_collection(child)
        elif child.tag == 'PLAYLISTS':
            assert playlists is None
            assert collection is not None
            playlists = _parse_playlists(child, collection)
        else:
            sys.stderr.write('WARNING: Unprocessed child: COLLECTION -> %s\n' % child.tag)

    assert collection is not None
    assert playlists is not None

    return collection, playlists

def main():
    collection, playlists = parse_library()
    return 0

if __name__ == '__main__':
    sys.exit(main())
