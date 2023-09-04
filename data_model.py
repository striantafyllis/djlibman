"""
Defines common classes such as Track, Playlist etc.
"""

import sys
from typing import Union
from collections import defaultdict

class Track:
    """Generic representation of a track on some platform, Rekordbox, Google sheet,
       Spotify, YouTube, etc. A track will always have a title and a list of artists.
       There will also be a platform-dependent ID (a hash in Rekordbox, a URI in Spotify etc.)
       that uniquely identifies the track. And then there will be some platform-dependent
       attributes about the track - e.g. BPM in Rekordbox or popularity score in Spotify."""
    id: Union[int, str]
    artists: frozenset[str]
    title: str
    attributes: dict[str, Union[str, int, float, list[str]]]
    foreign_keys: dict[str, Union[int, str]]

    def __init__(self, id, artists, title, attributes):
        self.id = id
        self.artists = artists
        self.title = title
        self.attributes = attributes

    def __str__(self):
        return "Track id: %s artists: '%s' title: '%s'" % (
            self.id,
            ','.join(self.artists),
            self.title
        )

class Tracklist(list[Track]):
    def __init__(self, tracks: list[Track]=[]):
        super(Tracklist, self).__init__(tracks)
        self._track_ids = set([track.id for track in tracks])

    def __str__(self):
        return '\n'.join([
            '%d: %s' % ((i+1), str(track))
            for i, track in enumerate(self.tracks)
            ])


class Playlist(Tracklist):
    """A generic playlist. A playlist will always have a name, and sometimes also a
       platform-dependent ID - e.g. a URI in Spotify."""
    id: Union[int, str]
    name: str
    def __init__(self, id: Union[int, str], name: str, tracks: list[Track] = []):
        super(Playlist, self).__init__(tracks)
        self.id = id
        self.name = name
        return

    def __str__(self):
        return "Playlist %s: '%s'\n" % (
            self.id,
            self.name
        ) + super(Playlist, self).__str()

class Library(Tracklist):
    """A generic library of tracks. Theoretically a library is unordered, but in practice
       the various libraries - Google sheet, Rekordbox etc. - sometimes have an order,
       so representing them as a tracklist is appropriate."""

    name: str
    attribute_types: dict[str, type]
    _tracks_by_id: dict[Union[int, str], Track]
    _tracks_by_artists_and_name: dict[frozenset[str], dict[str, Track]]

    def __init__(self, name: str):
        super(Library, self).__init__()
        self.name = name
        self.attributes = []
        self.attribute_types = {}
        self._tracks_by_id = {}
        self._tracks_by_artists_and_name = defaultdict(dict)

        return

    def append(self, track: Track):
        # Save the names and types of attributes
        for attribute, value in track.attributes.items():
            expected_type = self.attribute_types.get(attribute)
            if expected_type is None:
                self.attribute_types[attribute] = type(value)
            elif not isinstance(value, expected_type):
                raise Exception("Track %s attribute %s has value %s type %s; expected type %s" % (
                    track,
                    attribute,
                    value,
                    type(value).__name__,
                    expected_type.__name__
                ))

        if track.id in self._tracks_by_id:
            raise Exception('Duplicate track ID in library %s: %s' % (self.name, track.id))
        self._tracks_by_id[track.id] = track

        if track.title in self._tracks_by_artists_and_name[track.artists]:
            # this unfortunately happens so it can only be a warning; fix rekordbox library
            # so it doesn't happen
            sys.stderr.write("WARNING: Library %s: Duplicate artists and name: '%s' \u2013 '%s'\n" % (
                self.name, ', '.join(track.artists), track.title))
        self._tracks_by_artists_and_name[track.artists][track.title] = track

        super(Tracklist, self).append(track)
        return

    def get_track_by_id(self, id: Union[int, str]):
        return self._tracks_by_id.get(id)

    def get_track_by_artists_and_name(self, artists: frozenset[str], name: str):
        return self._tracks_by_artists_and_name[artists].get(name)







