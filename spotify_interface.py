import time
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

_TTL = 60

def is_spotify_id(s: str):
    # Spotify playlist IDs are base-62 numbers and they are usually about 22 digits long
    return len(s) > 20 and _BASE_62.match(s)


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
        logger.debug('Initializing Spotify connection with scope %s', _SCOPES)
        self._connection = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=','.join(_SCOPES),
                                                            cache_path=self._cached_token_file,
                                                            client_id=self._client_id,
                                                            client_secret=self._client_secret,
                                                            redirect_uri=self._redirect_uri))

        self._cache = cache.Cache()

        return

    def _wrap_request(self, request_name, **kwargs):
        start_time = time.time()
        result = getattr(self._connection, request_name)(**kwargs)
        end_time = time.time()
        logger.debug('Spotify request %s(%s): %.3f s',
                     request_name, kwargs, end_time - start_time)
        return result

    def _batch_result(self,
                      request_name,
                      **kwargs):
        start_time = time.time()

        result = getattr(self._connection, request_name)(**kwargs)

        items = result['items']

        num_batches = 1

        while result['next']:
            num_batches += 1

            result = self._connection.next(result)
            items += result['items']

        end_time = time.time()
        logger.debug('Spotify batch result %s(%s): %.3f s, items %d, batches %d',
                     request_name, kwargs, end_time - start_time, len(items), num_batches)

        return items

    def _batch_request(
            self,
            request_name, *,
            result_field=None,
            run_at_least_once=False,
            ** kwargs):
        if isinstance(request_name, tuple):
            first_request_name, subsequent_request_name = request_name
        else:
            first_request_name = request_name
            subsequent_request_name = request_name

        # find the one kwarg that is a list; that's the one we have to batch
        list_kwarg = None
        for kwarg in kwargs:
            if isinstance(kwargs[kwarg], list) or isinstance(kwargs[kwarg], pd.Series) or isinstance(kwargs[kwarg], pd.Index):
                if list_kwarg is None:
                    list_kwarg = kwarg
                else:
                    raise ValueError(f'More than one list argument: {list_kwarg}, {kwarg}')

        if list_kwarg is None:
            raise ValueError(f'No list arguments')

        items = kwargs[list_kwarg]

        results = [] if result_field is not None else None

        is_first = True

        start_time = time.time()
        num_batches = 0

        if len(items) == 0 and run_at_least_once:
            num_batches += 1
            result = getattr(self._connection, first_request_name)(**kwargs)
            if result_field is not None:
                results = result[result_field]

        start = 0
        while start < len(items):
            num_batches += 1

            end = min(start + _MAX_ITEMS_PER_REQUEST, len(items))

            batch = list(items[start:end])
            kwargs[list_kwarg] = batch
            if is_first:
                result = getattr(self._connection, first_request_name)(**kwargs)
            else:
                result = getattr(self._connection, subsequent_request_name)(**kwargs)
            is_first = False

            if result_field is not None:
                results += result[result_field]

            start = end

        end_time = time.time()

        logger.debug('Spotify batch request %s(%s): %.3f s, items %d, batches %d',
                     request_name, kwargs, end_time - start_time, len(items), num_batches)

        return results

    def invalidate_cache(self):
        self._cache = cache.Cache()
        return


    def get_user_id(self):
        time.time()
        return self._wrap_request('current_user')['id']
        logger.debug('Spotify request: current_user()')

    def get_playlists(self):
        def body():
            results = self._batch_result('current_user_playlists')

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
        self._wrap_request(
            'user_playlist_create',
            user=self.get_user_id(),
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
            results = self._batch_result('playlist_items', playlist_id=playlist_id)

            results = _postprocess_tracks(results)

            df = pd.DataFrame.from_records(results)
            if not df.empty:
                df = df.set_index('spotify_id', drop=False)

            return df

        return self._cache.look_up_or_get(body, _TTL, 'playlist_tracks', playlist_id)

    def get_liked_tracks(self):
        def body():
            results = self._batch_result('current_user_saved_tracks')

            results = _postprocess_tracks(results)

            df = pd.DataFrame.from_records(results)
            if not df.empty:
                df = df.set_index('spotify_id', drop=False)

            return df

        return self._cache.look_up_or_get(body, _TTL, 'liked_tracks')

    def get_artist_albums(self, artist_id):
        # this bypasses the cache; these results are not usually accessed multiple times
        # during a run

        results = self._batch_result('artist_albums', artist_id=artist_id)

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
        album_info = self._wrap_request('album', album_id=album_id)
        album_entry = project(album_info, _ALBUM_COLUMNS)

        results = self._batch_result('album_tracks', album_id=album_id)

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
        result = self._wrap_request('current_user_recently_played')

        results = result['items']

        results = _postprocess_tracks(results)

        df = pd.DataFrame.from_records(results)
        if not df.empty:
            df = df.set_index('spotify_id', drop=False)

        return df


    def get_tracks_by_id(self, ids, raw=False):
        results = self._batch_request(
            'tracks',
            tracks=ids,
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
        results = self._wrap_request('search', q=search_string, limit=limit)

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

        self._batch_request(
            'playlist_add_items',
            playlist_id=playlist_id,
            items=tracks
        )

        print("Added %d new tracks to playlist '%s'" % (len(tracks), playlist_name_or_id))

        self._cache.invalidate('playlist_tracks', playlist_id)

        return

    def replace_tracks_in_playlist(self, playlist_name_or_id, tracks):
        playlist_id = self._get_playlist_id_if_necessary(playlist_name_or_id)

        if isinstance(tracks, pd.DataFrame):
            tracks = tracks.spotify_id

        self._batch_request(
            ('playlist_replace_items', 'playlist_add_items'),
            playlist_id=playlist_id,
            items=tracks,
            run_at_least_once=True
        )

        print(f"Replaced contents of playlist '{playlist_name_or_id}' with {len(tracks)} tracks")

        self._cache.invalidate('playlist_tracks', playlist_id)

        return


    def remove_tracks_from_playlist(self, playlist_name_or_id, tracks):
        playlist_id = self._get_playlist_id_if_necessary(playlist_name_or_id)

        if isinstance(tracks, pd.DataFrame):
            tracks = tracks.spotify_id

        self._batch_request(
            'playlist_remove_all_occurrences_of_items',
            playlist_id=playlist_id,
            items=tracks
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

        self._batch_request(
            'current_user_saved_tracks_add',
            tracks=new_tracks
        )

        print('Added %d liked tracks' % len(new_tracks))

        self._cache.invalidate('liked_tracks')

        return

    def remove_liked_tracks(self, tracks):
        if isinstance(tracks, pd.DataFrame):
            tracks = tracks.spotify_id

        self._batch_request(
            'current_user_saved_tracks_delete',
            tracks=tracks
        )

        self._cache.invalidate('liked_tracks')

        return
