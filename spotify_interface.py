import os
import re
import spotipy
from spotipy.oauth2 import SpotifyOAuth

import pandas as pd
import numpy as np

_MAX_ITEMS_PER_REQUEST = 50

_SCOPES = [
    'user-library-read',
    'user-library-modify',
    'playlist-read-private',
    'playlist-read-collaborative',
    'playlist-modify-private',
    'playlist-modify-public',
    'user-library-read',
    'user-library-modify',
    'user-read-recently-played'
]

BASE_62 = re.compile(r'^[0-9A-Za-z]$')

class SpotifyInterface:
    def __init__(self, config):
        self._client_id = config['client_id']
        self._client_secret = config['client_secret']
        self._redirect_uri = config['redirect_uri']
        self._cached_token_file = config['cached_token_file']

        # this just creates the wrapper object; the actual network connection
        # will be initialized when we first try to use it
        self._connection = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=','.join(_SCOPES),
                                                            cache_path=self._cached_token_file,
                                                            client_id=self._client_id,
                                                            client_secret=self._client_secret,
                                                            redirect_uri=self._redirect_uri))

        return


    def get_connection(self):
        """Provides access to the raw Spotify interface. When possible, use one of the
        accessors below, which will also convert the results to Pandas DataFrames."""
        return self._connection

    def _results_wrapper(self, query, item_key=None, index=None):
        result = query
        items = result['items']

        while result['next']:
            result = self._connection.next(result)
            items += result['items']

        if item_key is not None:
            items = [item[item_key] for item in items]

        df = pd.DataFrame.from_records(items)

        if index is not None:
            df = df.set_index(index, drop=False)

        return df

    def get_playlists(self):
        return self._results_wrapper(
            self._connection.current_user_playlists(),
            index='name')

    def get_playlist_tracks(self, playlist_name_or_id):
        # Spotify playlist IDs are base-62 numbers and they are usually about 22 digits long
        if len(playlist_name_or_id) > 20 and BASE_62.match(playlist_name_or_id):
            playlist_id = playlist_name_or_id
        else:
            # the string is a playlist name
            playlists = self.get_playlists()

            if playlist_name_or_id not in playlists.index:
                raise Exception("Spotify playlist '%s' not found" % playlist_name_or_id)

            playlist_id = playlists.at[playlist_name_or_id, 'id']

        df = self._results_wrapper(
            self._connection.playlist_items(playlist_id),
            item_key='track',
            index='id')

        # add artist names for convenience
        df['artist_names'] = df.artists.apply(lambda artists: [artist['name'] for artist in artists])

        return df

    def get_liked_tracks(self):
        df = self._results_wrapper(
            self._connection.current_user_saved_tracks(),
            item_key='track',
            index='id')

        # add artist names for convenience
        df['artist_names'] = df.artists.apply(lambda artists: [artist['name'] for artist in artists])

        return df

