#!/usr/bin/env python

import sys
import re
from dataclasses import dataclass
import xml.etree.ElementTree as ET
import urllib.parse
from collections import defaultdict


default_playlist_dir = '/Users/spyros/Music/'
default_rekordbox_xml = default_playlist_dir + 'rekordbox.xml'

@dataclass
class Track:
    id: int
    artist_orig: str
    artists: list[str]
    title: str
    duration: int
    bpm: float
    date_added: str
    bit_rate: int
    location: str
    key: str
    track_info = None

    def __str__(self):
        return "Track id: %d artists: '%s' title: '%s'" % (
            self.id,
            ','.join(self.artists),
            self.title
        )

class Playlist:
    name: str
    tracks: list[Track]

    def __init__(self, name: str, tracks: list[Track]):
        self.name = name
        self.tracks = tracks
        self.track_ids = frozenset([track.id for track in tracks])

    def __contains__(self, track):
        return track.id in self.track_ids


class Collection:
    def __init__(self, **kwargs):
        self.tracks_by_id = {}
        self.tracks_by_artists_and_name = defaultdict(dict)
        return

    def add_track(self, track: Track) -> None:
        if track.id in self.tracks_by_id:
            raise Exception('Duplicate track ID: %d' % track.id)
        self.tracks_by_id[track.id] = track

        artists = frozenset(track.artists)
        if track.title in self.tracks_by_artists_and_name[artists]:
            # this unfortunately happens so it can only be a warning; fix rekordbox library
            # so it doesn't happen
            sys.stderr.write("WARNING: Duplicate artists and name: '%s' - '%s'\n" % (artists, track.title))
        self.tracks_by_artists_and_name[artists][track.title] = track

        return

    def all_tracks_by_id(self):
        tracks = list(self.tracks_by_id.values())
        tracks.sort(key = lambda t: t.id)
        return tracks


def expect_str(node, key):
    value = node.attrib.get(key)
    if value is None:
        raise Exception("Missing attribute '%s' in node %s" % (key, node))
    return value

def expect_int(node, key):
    return int(expect_str(node, key))

def expect_float(node, key):
    return float(expect_str(node, key))

def expect_filename(node, key):
    value = expect_str(node, key)
    if value.startswith('file://localhost'):
        value = urllib.parse.unquote(value[16:])
    return value


def parse_collection(node: ET.Element):
    assert node.tag ==  'COLLECTION'
    collection = Collection()

    for child in node:
        if child.tag == 'TRACK':
            track = parse_rekordbox_collection_track(child)
            collection.add_track(track)
        else:
            raise Exception('Unknown tag %s in node COLLECTION' % child.tag)

    return collection

def parse_rekordbox_collection_track(node: ET.Element):
    assert node.tag == 'TRACK'

    return Track(
        id = expect_int(node, 'TrackID'),
        artist_orig = expect_str(node, 'Artist'),
        artists = re.split(r' *[,&] *', expect_str(node, 'Artist')),
        title= expect_str(node, 'Name'),
        duration = expect_int(node, 'TotalTime'),
        bpm = expect_float(node, 'AverageBpm'),
        date_added = expect_str(node, 'DateAdded'),
        bit_rate = expect_int(node, 'BitRate'),
        location = expect_filename(node, 'Location'),
        key = expect_str(node, 'Tonality')
    )

def parse_playlists(node: ET.Element, collection: Collection):
    assert node.tag == 'PLAYLISTS'

    all_playlists = []

    for child in node:
        all_playlists += parse_playlist_node(child, collection, [])

    playlists = {}

    for playlist in all_playlists:
        assert playlist.name not in playlists
        playlists[playlist.name] = playlist

    return playlists

def parse_playlist_node(node: ET.Element,
                        collection: Collection,
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
            all_playlists += parse_playlist_node(child, collection, prefix)

        return all_playlists
    elif type == '1':
        # leaf playlist
        full_name = '/'.join(prefix + [name])

        tracks = []

        for child in node:
            assert child.tag == 'TRACK'
            track_id = int(child.attrib['Key'])

            track = collection.tracks_by_id.get(track_id)
            if track is None:
                raise Exception('Unknown track ID %d in playlist %s' % (track_id, full_name))

            tracks.append(track)

        return [Playlist(full_name, tracks)]


def debug_print_xml_node(node, indent=0):
    print(" "*indent + "tag='%s' attrib=%s children=%d" % (node.tag, node.attrib, len(node)))
    # tag: string
    # attrib: dict
    for child in node:
        debug_print_xml_node(child, indent+4)
    return


def parse_library(library_file=default_rekordbox_xml):
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
            collection = parse_collection(child)
        elif child.tag == 'PLAYLISTS':
            assert playlists is None
            assert collection is not None
            playlists = parse_playlists(child, collection)
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
