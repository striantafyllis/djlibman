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
    'user-read-recently-played'
]

BASE_62 = re.compile(r'^[0-9A-Za-z]+$')

def _batch_result(request_func, start=0, stop=None, stop_condition=None, use_offset=True):
    """Stop condition is inclusive"""
    if start < 0:
        raise ValueError('Negative start: %s' % start)

    if stop is None:
        stop = 1000000000
    elif stop < 0:
        raise ValueError('Negative stop: %s' % stop)
    elif stop < start:
        raise ValueError('Stop is less than start: [%s, %s]' % (start, stop))

    all_items = []

    while True:
        limit = min(_MAX_ITEMS_PER_REQUEST, stop-start)

        result = request_func(offset=start, limit=limit)
        items = result['items']

        stop_idx = None

        if stop_condition is not None:
            for idx, item in enumerate(items):
                if stop_condition(item):
                    stop_idx = idx+1
                    break

        if stop_idx is not None:
            all_items += items[:stop_idx]
            break
        else:
            all_items += items

        if stop is not None and len(all_items) >= (stop-start):
            break

        if not result['next']:
            break

        start += len(items)

    return all_items


def _batch_request(items, request):
    start = 0

    while start < len(items):
        end = min(start + _MAX_ITEMS_PER_REQUEST, len(items))

        batch = list(items[start:end])
        request(batch)

        start = end

    return

def _postprocess_tracks(results):
    """Apply some common manipulations to Spotify API returns involving tracks"""

    for result in results:
        # flatten the 'track' field
        track_fields = result['track']
        del result['track']
        result.update(track_fields)

        # add artist names for convenience
        result['artist_names'] = [artist['name'] for artist in result['artists']]

        # convert timestamps
        for field in ['added_at', 'played_at']:
            if field in result:
                result[field] = pd.to_datetime(result[field])

    return results


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

    def get_playlists(self, start=0, stop=None, stop_condition=None):
        results = _batch_result(
            lambda limit, offset: self._connection.current_user_playlists(limit, offset),
            start, stop, stop_condition
        )

        df = pd.DataFrame.from_records(results)
        df = df.set_index(df.name)

        return df


    def _get_playlist_id(self, playlist_name_or_id):
        # Spotify playlist IDs are base-62 numbers and they are usually about 22 digits long
        if len(playlist_name_or_id) > 20 and BASE_62.match(playlist_name_or_id):
            return playlist_name_or_id

        # the string is a playlist name
        playlists = self.get_playlists(stop_condition=lambda item: item['name'] == playlist_name_or_id)

        if playlist_name_or_id not in playlists.index:
            raise Exception("Spotify playlist '%s' not found" % playlist_name_or_id)

        return playlists.at[playlist_name_or_id, 'id']

    def get_playlist_tracks(self, playlist_name_or_id, start=0, stop=None, stop_condition=None):
        playlist_id = self._get_playlist_id(playlist_name_or_id)

        results = _batch_result(
            lambda limit, offset: self._connection.playlist_items(
                playlist_id=playlist_id,
                limit=limit, offset=offset
            ),
            start, stop, stop_condition)

        _postprocess_tracks(results)

        df = pd.DataFrame.from_records(results)
        df = df.set_index(df.id)

        return df

    def get_liked_tracks(self, start=0, stop=None, stop_condition=None):
        results = _batch_result(
            lambda limit, offset: self._connection.current_user_saved_tracks(
                limit=limit, offset=offset
            ),
            start, stop, stop_condition)

        _postprocess_tracks(results)

        df = pd.DataFrame.from_records(results)
        df = df.set_index(df.id)

        return df

    def get_recently_played_tracks(self):
        """Access the last played tracks, up to 50; Spotify doesn't give us access to more."""
        result = self._connection.current_user_recently_played()

        results = result['items']

        _postprocess_tracks(results)

        df = pd.DataFrame.from_records(results)
        df = df.set_index(df.id)

        return df


    def add_tracks_to_playlist(self, playlist_name_or_id, tracks):
        playlist_id = self._get_playlist_id(playlist_name_or_id)

        if isinstance(tracks, pd.DataFrame):
            tracks = tracks.id

        _batch_request(
            tracks,
            lambda x: self._connection.playlist_add_items(playlist_id, x)
        )

        return

    def remove_tracks_from_playlist(self, playlist_name_or_id, tracks):
        playlist_id = self._get_playlist_id(playlist_name_or_id)

        if isinstance(tracks, pd.DataFrame):
            tracks = tracks.id

        _batch_request(
            tracks,
            lambda x: self._connection.playlist_remove_all_occurrences_of_items(playlist_id=playlist_id, items=x)
        )

        return

