import os
import re
import spotipy
from spotipy.oauth2 import SpotifyOAuth

import pandas as pd
import numpy as np

from internal_utils import *

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

_ARTIST_COLUMNS = ['id', 'name']

_ALBUM_COLUMNS = {
        'id': None,
        'name': None,
        'album_type': None,
        'release_date': lambda item: pd.to_datetime(item['release_date'], utc=True),
        'artists': _ARTIST_COLUMNS
    }

_TRACK_COLUMNS = {
    'id': None,
    'name': None,
    'artists': _ARTIST_COLUMNS,
    'duration_ms': None,
    'popularity': None,
    'added_at': lambda item: pd.to_datetime(
        item['played_at'] if 'played_at' in item else
            (item['added_at'] if 'added_at' in item else None),
        utc=True),
    'added_by': lambda item: item['added_by']['id'] if 'added_by' in item else None,
    'album': _ALBUM_COLUMNS,
    'disc_number': None,
    'track_number': None,
    # ignored columns from Spotify:
    # 'is_local',
    # 'primary_color',
    # 'video_thumbnail',
    # 'preview_url',
    # 'available_markets',
    # 'explicit',
    # 'type',
    # 'episode',
    # 'track',
    # 'external_ids',
    # 'external_urls',
    # 'href',
    # 'uri'
}

BASE_62 = re.compile(r'^[0-9A-Za-z]+$')

def _batch_result(request_func, start=0, stop=None, stop_condition=None, use_offset=True):
    """Stop condition is inclusive"""

    assert stop is None or stop_condition is None

    if start < 0:
        raise ValueError('Negative start: %s' % start)

    if stop is None:
        stop = 1000000000
    elif stop < 0:
        raise ValueError('Negative stop: %s' % stop)
    elif stop < start:
        raise ValueError('Stop is less than start: [%s, %s]' % (start, stop))

    all_items = []

    offset = start

    while True:
        limit = min(_MAX_ITEMS_PER_REQUEST, stop-offset)

        result = request_func(offset=offset, limit=limit)
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

        offset += len(items)

    return all_items


def _batch_request(request, items, result_field=None):
    start = 0

    results = [] if result_field is not None else None

    while start < len(items):
        end = min(start + _MAX_ITEMS_PER_REQUEST, len(items))

        batch = list(items[start:end])
        result = request(batch)

        if result_field is not None:
            results += result[result_field]

        start = end

    return results

def _postprocess_tracks(results):
    """Apply some common manipulations to Spotify API returns involving tracks"""

    for result in results:
        # flatten the 'track' field
        if 'track' in result:
            track_fields = result['track']
            del result['track']
            result.update(track_fields)

    projection = project(results, _TRACK_COLUMNS)
    return projection


class SpotifyInterface:
    def __init__(self, config):
        self._client_id = config['client_id']

        client_secret_loc = config['client_secret']

        if client_secret_loc.startswith('$'):
            self._client_secret = os.environ[client_secret_loc[1:]]
        else:
            with open(client_secret_loc) as client_secret_file:
                self._client_secret = client_secret_file.read().strip()

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

    def get_user_id(self):
        return self._connection.current_user()['id']

    def get_playlists(self, start=0, stop=None, stop_condition=None, raw=False):
        results = _batch_result(
            lambda limit, offset: self._connection.current_user_playlists(limit, offset),
            start, stop, stop_condition
        )

        if raw:
            return results

        df = pd.DataFrame.from_records(results)
        df = df.set_index(df.name)

        return df

    def get_playlist_id(self, playlist_name):
        playlists = self.get_playlists(stop_condition=lambda item: item['name'] == playlist_name)

        if playlist_name not in playlists.index:
            raise Exception("Spotify playlist '%s' not found" % playlist_name)

        return playlists.at[playlist_name, 'id']

    def add_playlist(self, playlist_name):
        self._connection.user_playlist_create(user=self._connection.current_user(),
                                              name=playlist_name,
                                              public=False,
                                              collaborative=False)
        return

    def _get_playlist_id_if_necessary(self, playlist_name_or_id):
        # Spotify playlist IDs are base-62 numbers and they are usually about 22 digits long
        if len(playlist_name_or_id) > 20 and BASE_62.match(playlist_name_or_id):
            return playlist_name_or_id

        # the string is a playlist name
        return self.get_playlist_id(playlist_name_or_id)

    def get_playlist_tracks(self, playlist_name_or_id, start=0, stop=None, stop_condition=None,
                            up_to_track=None, raw=False):
        if up_to_track is not None:
            assert stop is None
            stop_condition = lambda item: item['track']['name'] == up_to_track

        playlist_id = self._get_playlist_id_if_necessary(playlist_name_or_id)

        results = _batch_result(
            lambda limit, offset: self._connection.playlist_items(
                playlist_id=playlist_id,
                limit=limit, offset=offset
            ),
            start, stop, stop_condition)

        if raw:
            return results

        results = _postprocess_tracks(results)

        df = pd.DataFrame.from_records(results)
        if not df.empty:
            df = df.set_index(df.id)

        return df

    def get_liked_tracks(self, start=0, stop=None, stop_condition=None, raw=False):
        results = _batch_result(
            lambda limit, offset: self._connection.current_user_saved_tracks(
                limit=limit, offset=offset
            ),
            start, stop, stop_condition)

        if raw:
            return results

        results = _postprocess_tracks(results)

        df = pd.DataFrame.from_records(results)
        if not df.empty:
            df = df.set_index(df.id)

        return df

    def get_recently_played_tracks(self, raw=False):
        """Access the last played tracks, up to 50; Spotify doesn't give us access to more."""
        result = self._connection.current_user_recently_played()

        results = result['items']

        if raw:
            return results

        results = _postprocess_tracks(results)

        df = pd.DataFrame.from_records(results)
        if not df.empty:
            df = df.set_index(df.id)

        return df


    def get_tracks_by_id(self, ids, raw=False):
        results = _batch_request(
            lambda items: self._connection.tracks(items),
            ids,
            result_field = 'tracks'
        )

        if raw:
            return results

        results = _postprocess_tracks(results)

        df = pd.DataFrame.from_records(results)
        if not df.empty:
            df = df.set_index(df.id)

        return df

    def search(self, search_string, limit=10, raw=False):
        # no batching
        results = self._connection.search(q=search_string, limit=limit)

        if raw:
            return results

        results = results['tracks']['items']

        results = _postprocess_tracks(results)

        df = pd.DataFrame.from_records(results)
        if not df.empty:
            df = df.set_index(df.id)

        return df

    def add_tracks_to_playlist(self, playlist_name_or_id, tracks, check_for_duplicates=True):
        playlist_id = self._get_playlist_id_if_necessary(playlist_name_or_id)

        if isinstance(tracks, pd.DataFrame):
            tracks = tracks.id

        if check_for_duplicates:
            existing_tracks = self.get_playlist_tracks(playlist_id)

            new_tracks = [track_id for track_id in tracks if track_id not in existing_tracks]

            if len(new_tracks) != len(tracks):
                print("Ignoring %d tracks that already exist in playlist '%s'" % (
                    len(tracks) - len(new_tracks),
                    playlist_name_or_id
                ))

            tracks = new_tracks

        _batch_request(
            lambda x: self._connection.playlist_add_items(playlist_id, x),
            tracks
        )

        print("Added %d new tracks to playlist '%s'" % (len(tracks), playlist_name_or_id))

        return

    def remove_tracks_from_playlist(self, playlist_name_or_id, tracks):
        playlist_id = self._get_playlist_id_if_necessary(playlist_name_or_id)

        if isinstance(tracks, pd.DataFrame):
            tracks = tracks.id

        _batch_request(
            lambda x: self._connection.playlist_remove_all_occurrences_of_items(playlist_id=playlist_id, items=x),
            tracks
        )

        return

    def add_liked_tracks(self, tracks):
        if isinstance(tracks, pd.DataFrame):
            tracks = tracks.id

        if not isinstance(tracks, pd.Index):
            tracks = pd.Index(tracks)

        # don't add tracks that are already liked because it messes up the added_at timestamp
        already_liked_tracks = self.get_liked_tracks().index

        new_tracks = tracks.difference(already_liked_tracks, sort=False)

        if len(new_tracks) < len(tracks):
            print('Ignoring %d already liked tracks' % (len(tracks) - len(new_tracks)))

        _batch_request(
            lambda x: self._connection.current_user_saved_tracks_add(x),
            new_tracks
        )

        print('Added %d liked tracks' % len(new_tracks))

        return

    def remove_liked_tracks(self, tracks):
        if isinstance(tracks, pd.DataFrame):
            tracks = tracks.id

        _batch_request(
            lambda x: self._connection.current_user_saved_tracks_delete(x),
            tracks
        )

        return





