"""
Functions to maintain artist discographies.
These need special treatment because they generate too many Spotify REST requests,
which are slow, and may also lead Spotify to rate-limit me.

In the Spotify API, getting an artist's discography has to be done in two steps:
- First, get all the albums that contain the artist's tracks
- Then, get all the tracks for each album, and filter out the artist's tracks

Both these stages can be cached to disk, so that they don't have to be repeated every time.
- Album tracks should never change; once recovered, they can be used forever.
- An artist's albums can change, but there is no need to refresh them very frequently;
  once a week or so should be enough.
"""

import time
import logging

import djlib_config
from containers import *
from spotify_util import *

logger = logging.getLogger(__name__)

_singleton = None

class _SpotifyDiscography:
    def __init__(self):
        self._artists = None
        return

    def _init_artists(self):
        if self._artists is not None:
            return

        start_time = time.time()

        listening_history = ListeningHistory()

        self._artists = get_track_artists(listening_history)

        return

    def get_spotify_artists(self):
        self._init_artists()
        return self._artists


def get_instance():
    global _singleton
    if _singleton is None:
        _singleton = _SpotifyDiscography()
    return _singleton

