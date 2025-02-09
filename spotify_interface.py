import os
import re
import logging
import spotipy
from spotipy.oauth2 import SpotifyOAuth

import pandas as pd
import numpy as np

import cache
from general_utils import *

logger = logging.getLogger(__name__)

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

_ALBUM_COLUMNS = {
    'album_id': 'id',
    'name': None,
    'artist_ids': lambda t: '|'.join([artist['id'] for artist in t['artists']]),
    # in the unlikely case that the artist name contains a pipe...
    'artist_names': lambda t: '|'.join([artist['name'].replace('|', '--') for artist in t['artists']]),
    'popularity': lambda t: t['popularity'] if 'popularity' in t else None,
    'album_type': None,
    'release_date': lambda item: pd.to_datetime(item['release_date'], utc=True),
    'total_tracks': lambda item: int(item['total_tracks'])
}

_TRACK_COLUMNS = {
    'spotify_id': 'id',
    'name': None,
    'artist_ids': lambda t: '|'.join([artist['id'] for artist in t['artists']]),
    # in the unlikely case that the artist name contains a pipe...
    'artist_names': lambda t: '|'.join([artist['name'].replace('|', '--') for artist in t['artists']]),
    'duration_ms': int,
    'release_date': lambda t: pd.to_datetime(t['album']['release_date'], utc=True),
    'popularity': int,
    'added_at': lambda item: pd.to_datetime(
        item['played_at'] if 'played_at' in item else
            (item['added_at'] if 'added_at' in item else None),
        utc=True),
    'album_id': lambda t: t['album']['id'],
    'album_name': lambda t: t['album']['name'],
    # ignored columns from Spotify:
    # 'added_by',
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


_BASE_62 = re.compile(r'^[0-9A-Za-z]+$')

_TTL = 600

def is_spotify_id(s: str):
    # Spotify playlist IDs are base-62 numbers and they are usually about 22 digits long
    return len(s) > 20 and _BASE_62.match(s)


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


def _batch_request(request, items, result_field=None, run_at_least_once=False):
    start = 0

    if isinstance(request, tuple):
        first_request, subsequent_request = request
    else:
        first_request = request
        subsequent_request = request

    results = [] if result_field is not None else None

    is_first = True

    if len(items) == 0 and run_at_least_once:
        first_request([])

    while start < len(items):
        end = min(start + _MAX_ITEMS_PER_REQUEST, len(items))

        batch = list(items[start:end])
        if is_first:
            result = first_request(batch)
        else:
            result = subsequent_request(batch)
        is_first = False

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

def _postprocess_albums(results):
    projection = project(results, _ALBUM_COLUMNS)
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

        self._cache = cache.Cache()

        return


    def get_connection(self):
        """Provides access to the raw Spotify interface. When possible, use one of the
        accessors below, which will also convert the results to Pandas DataFrames."""
        return self._connection

    def get_user_id(self):
        return self._connection.current_user()['id']

    def get_playlists(self):
        def body():
            results = _batch_result(
                lambda limit, offset: self._connection.current_user_playlists(limit, offset))

            df = pd.DataFrame.from_records(results)
            df = df.set_index(df.name)

            return df

        return self._cache.look_up_or_get(body, _TTL, 'playlists')

    def playlist_exists(self, playlist_name):
        return playlist_name in self.get_playlists().index

    def get_playlist_id(self, playlist_name):
        playlists = self.get_playlists()

        if playlist_name not in playlists.index:
            raise Exception("Spotify playlist '%s' not found" % playlist_name)

        playlist_id = playlists.at[playlist_name, 'id']

        if isinstance(playlist_id, pd.Series):
            raise Exception(f"There are '{len(playlist_id)}' playlists with name '{playlist_name}'")
        elif not isinstance(playlist_id, str):
            assert False

        return playlist_id

    def create_playlist(self, playlist_name):
        self._connection.user_playlist_create(user=self._connection.current_user()['id'],
                                              name=playlist_name,
                                              public=False,
                                              collaborative=False)
        self._cache.invalidate('playlists')
        return

    def _get_playlist_id_if_necessary(self, playlist_name_or_id):
        if is_spotify_id(playlist_name_or_id):
            return playlist_name_or_id

        # the string is a playlist name
        return self.get_playlist_id(playlist_name_or_id)

    def get_playlist_tracks(self, playlist_name_or_id):
        playlist_id = self._get_playlist_id_if_necessary(playlist_name_or_id)

        def body():
            results = _batch_result(
                lambda limit, offset: self._connection.playlist_items(
                    playlist_id=playlist_id,
                    limit=limit, offset=offset
                ))

            results = _postprocess_tracks(results)

            df = pd.DataFrame.from_records(results)
            if not df.empty:
                df = df.set_index('spotify_id', drop=False)

            return df

        return self._cache.look_up_or_get(body, _TTL, 'playlist_tracks', playlist_id)

    def get_liked_tracks(self):
        def body():
            results = _batch_result(
                lambda limit, offset: self._connection.current_user_saved_tracks(
                    limit=limit, offset=offset
                ))

            results = _postprocess_tracks(results)

            df = pd.DataFrame.from_records(results)
            if not df.empty:
                df = df.set_index('spotify_id', drop=False)

            return df

        return self._cache.look_up_or_get(body, _TTL, 'liked_tracks')

    def get_artist_albums(self, artist_id):
        # this bypasses the cache; these results are not usually accessed multiple times
        # during a run

        results = _batch_result(
            lambda limit, offset: self._connection.artist_albums(artist_id=artist_id, limit=limit, offset=offset)
        )

        results = _postprocess_albums(results)

        df = pd.DataFrame.from_records(results)
        if not df.empty:
            df = df.set_index('album_id', drop=False)

        return df

    def get_album_tracks(self, album_id):
        # this bypasses the cache; these results are not usually accessed multiple times
        # during a run

        # note: we need to repeat the album query even if we have the album entry from
        # get_artist_albums() because the get_artist_albums() result misses the popularity field
        album_info = self._connection.album(album_id)
        album_entry = project(album_info, _ALBUM_COLUMNS)

        results = _batch_result(
            lambda limit, offset: self._connection.album_tracks(album_id=album_id, limit=limit, offset=offset)
        )

        # make these look like the track lists that come back from playlists etc.
        projection = project(
            results,
            {
                'spotify_id': 'id',
                'name': None,
                'artist_ids': lambda t: '|'.join([artist['id'] for artist in t['artists']]),
                # in the unlikely case that the artist name contains a pipe...
                'artist_names': lambda t: '|'.join([artist['name'].replace('|', '--') for artist in t['artists']]),
                'duration_ms': int,
                'release_date': lambda _: album_entry['release_date'],
                'popularity': lambda _: album_entry['popularity'],
                'added_at': lambda _: album_entry['release_date'],
                'album_id': lambda _: album_id,
                'album_name': lambda _: album_entry['name']
            }
        )

        df = pd.DataFrame.from_records(projection)
        if not df.empty:
            df = df.set_index('spotify_id', drop=False)

        return df


    def get_recently_played_tracks(self):
        """Access the last played tracks, up to 50; Spotify doesn't give us access to more."""
        result = self._connection.current_user_recently_played()

        results = result['items']

        results = _postprocess_tracks(results)

        df = pd.DataFrame.from_records(results)
        if not df.empty:
            df = df.set_index('spotify_id', drop=False)

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
            df = df.set_index('spotify_id', drop=False)

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
            df = df.set_index('spotify_id', drop=False)

        return df

    def add_tracks_to_playlist(self, playlist_name_or_id, tracks, check_for_duplicates=True):
        playlist_id = self._get_playlist_id_if_necessary(playlist_name_or_id)

        if isinstance(tracks, pd.DataFrame):
            tracks = tracks.spotify_id

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

        self._cache.invalidate('playlist_tracks', playlist_id)

        return

    def replace_tracks_in_playlist(self, playlist_name_or_id, tracks):
        playlist_id = self._get_playlist_id_if_necessary(playlist_name_or_id)

        if isinstance(tracks, pd.DataFrame):
            tracks = tracks.spotify_id

        _batch_request(
            (lambda x: self._connection.playlist_replace_items(playlist_id, x),
             lambda x: self._connection.playlist_add_items(playlist_id, x)
             ),
            tracks,
            run_at_least_once=True
        )

        print(f"Replaced contents of playlist '{playlist_name_or_id}' with {len(tracks)} tracks")

        self._cache.invalidate('playlist_tracks', playlist_id)

        return


    def remove_tracks_from_playlist(self, playlist_name_or_id, tracks):
        playlist_id = self._get_playlist_id_if_necessary(playlist_name_or_id)

        if isinstance(tracks, pd.DataFrame):
            tracks = tracks.spotify_id

        _batch_request(
            lambda x: self._connection.playlist_remove_all_occurrences_of_items(playlist_id=playlist_id, items=x),
            tracks
        )

        self._cache.invalidate('playlist_tracks', playlist_id)

        return

    def add_liked_tracks(self, tracks):
        if isinstance(tracks, pd.DataFrame):
            tracks = tracks.spotify_id

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

        self._cache.invalidate('liked_tracks')

        return

    def remove_liked_tracks(self, tracks):
        if isinstance(tracks, pd.DataFrame):
            tracks = tracks.spotify_id

        _batch_request(
            lambda x: self._connection.current_user_saved_tracks_delete(x),
            tracks
        )

        self._cache.invalidate('liked_tracks')

        return
