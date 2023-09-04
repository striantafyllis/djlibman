"""
Defines common classes such as Track, Playlist etc.
"""

from typing import Union

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

    def __init__(self, id, artists, title):
        self.id = id
        self.artists = artists
        self.title = title

    def __str__(self):
        return "Track id: %s artists: '%s' title: '%s'" % (
            self.id,
            ','.join(self.artists),
            self.title
        )

class Tracklist(list[Track]):
    _track_ids: set

    def __init__(self, tracks: list[Track]=[]):
        super(Tracklist, self).__init__(tracks)
        self._track_ids = set([track.id for track in tracks])

    def append(self, track: Track):
        if track.id in self._track_ids:
            raise Exception('Duplicate track ID in tracklist: %s' % track.id)
        self._track_ids.add(track.id)
        super(Tracklist, self).append(track)
        return

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

    def __init__(self, id: Union[int, str], name: str, tracks: list[Track] = None):
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
    attributes: list[str]  # the attributes, in the order they appear in the sheet, XML, JSON etc.
    attribute_types: dict[str, type]

    def __init__(self, name: str, tracks: list[Track] = None):
        super(Library, self).__init__(tracks)
        self.name = name
        return







