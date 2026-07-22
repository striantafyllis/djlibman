import time
import logging
import json
import base64
import hashlib
import os
import os.path
import re
import random
import sys
import webbrowser
from urllib.parse import urlencode, urlparse, parse_qs
import requests

import pandas as pd
import numpy as np

from spyroslib import cache
from local_util import *

logger = logging.getLogger(__name__)

_MAX_ITEMS_PER_REQUEST = 20

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
}

_BASE_62 = re.compile(r'^[0-9A-Za-z]+$')

_TTL = 60

def is_spotify_id(s: str):
    return len(s) > 20 and _BASE_62.match(s)

def _postprocess_tracks(results):
    for result in results:
        if 'track' in result:
            track_fields = result['track']
            del result['track']
            result.update(track_fields)
    projection = project(results, _TRACK_COLUMNS)
    return projection

def _postprocess_albums(results):
    projection = project(results, _ALBUM_COLUMNS)
    return projection


def _generate_code_verifier(length: int = 128) -> str:
    possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
    return ''.join(random.choices(possible, k=length))

def _generate_code_challenge(code_verifier: str) -> str:
    digest = hashlib.sha256(code_verifier.encode('utf-8')).digest()
    return base64.urlsafe_b64encode(digest).decode('utf-8').rstrip('=')


class SpotifyInterface:
    def __init__(self, config):
        self._client_id = config['client_id']
        if self._client_id.startswith('$'):
            self._client_id = os.environ[self._client_id[1:]]

        client_secret_loc = config['client_secret']
        if client_secret_loc.startswith('$'):
            self._client_secret = os.environ[client_secret_loc[1:]]
        else:
            with open(client_secret_loc) as client_secret_file:
                self._client_secret = client_secret_file.read().strip()

        self._redirect_uri = config.get('redirect_uri')
        if self._redirect_uri.startswith('$'):
            self._redirect_uri = os.environ.get(self._redirect_uri[1:])

        self._cached_token_file = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            config['cached_token_file']
        )

        self._access_token = None
        self._refresh_token = None
        self._access_token_expires_at = None

        self._cache = cache.Cache()
        return

    def _ensure_access_token(self):
        if self._access_token is None:
            if os.path.exists(self._cached_token_file):
                try:
                    self._read_access_token_file()
                except Exception as e:
                    logger.debug('Reading Spotify access token file failed: %s', str(e))
                    self._authorization_workflow()

        if self._access_token_expires_at is not None and \
            time.time() < self._access_token_expires_at - 60:
            return

        if self._refresh_token is not None:
            try:
                self._refresh_token_workflow()
                return
            except Exception as e:
                logger.debug('Refreshing Spotify access token failed: %s', str(e))

        self._authorization_workflow()
        return

    def _read_access_token_file(self):
        with open(self._cached_token_file) as token_fh:
            obj = json.load(token_fh)
            self._access_token = obj['access_token']
            self._refresh_token = obj['refresh_token']
            self._access_token_expires_at = obj['expires_at']

    def _write_access_token_file(self):
        with open(self._cached_token_file, 'w') as token_file:
            json.dump(
                obj={
                    'access_token': self._access_token,
                    'token_type': 'Bearer',
                    'expires_in': 3600,
                    'scope': ' '.join(_SCOPES),
                    'expires_at': int(self._access_token_expires_at),
                    'refresh_token': self._refresh_token
                },
                fp=token_file,
                indent=2
            )

    def _authorization_workflow(self):
        state = ''.join(random.choices('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=10))
        code_verifier = _generate_code_verifier(128)
        code_challenge = _generate_code_challenge(code_verifier)

        options = {
            'client_id': self._client_id,
            'response_type': 'code',
            'redirect_uri': self._redirect_uri,
            'scope': ' '.join(_SCOPES),
            'state': state,
            'code_challenge_method': 'S256',
            'code_challenge': code_challenge
        }

        authorize_url = f'https://accounts.spotify.com/authorize?{urlencode(options)}'
        print(f'Authorize URL: {authorize_url}')
        webbrowser.open(authorize_url)

        sys.stdout.write('Paste redirect URL here: > ')
        sys.stdout.flush()

        redirect_url = sys.stdin.readline().strip()

        parse_result = urlparse(redirect_url)
        response_options = parse_qs(parse_result.query)

        response_state = response_options['state'][0]
        response_code = response_options['code'][0]

        if response_state != state:
            raise Exception(f"Mismatch in state part of response; send '{state}', received '{response_state}'")

        post_data = {
            'grant_type': 'authorization_code',
            'code': response_code,
            'redirect_uri': self._redirect_uri,
            'client_id': self._client_id,
            'code_verifier': code_verifier
        }

        auth_header = base64.b64encode(f"{self._client_id}:{self._client_secret}".encode()).decode()
        post_headers = {
            'Authorization': f'Basic {auth_header}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        post_response = requests.post(
            url='https://accounts.spotify.com/api/token',
            data=post_data,
            headers=post_headers
        )

        if post_response.status_code != 200:
            raise Exception(f'Spotify authorization flow failed; '
                            f'code {post_response.status_code} text {post_response.text}')

        post_response_data = post_response.json()
        self._access_token = post_response_data['access_token']
        self._refresh_token = post_response_data['refresh_token']
        expires_in = post_response_data['expires_in']
        self._access_token_expires_at = time.time() + expires_in

        self._write_access_token_file()

    def _refresh_token_workflow(self):
        assert self._refresh_token is not None

        post_data = {
            'grant_type': 'refresh_token',
            'refresh_token': self._refresh_token
        }

        auth_header = base64.b64encode(f"{self._client_id}:{self._client_secret}".encode()).decode()
        post_headers = {
            'Authorization': f'Basic {auth_header}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        post_response = requests.post(
            url='https://accounts.spotify.com/api/token',
            data=post_data,
            headers=post_headers
        )

        if post_response.status_code != 200:
            raise Exception(f'Spotify refresh token flow failed; '
                            f'code {post_response.status_code} text {post_response.text}')

        post_response_data = post_response.json()
        self._access_token = post_response_data['access_token']
        if 'refresh_token' in post_response_data:
            self._refresh_token = post_response_data['refresh_token']
        expires_in = post_response_data['expires_in']
        self._access_token_expires_at = time.time() + expires_in

        self._write_access_token_file()

    def _api_request(self, method, url, params=None, json_data=None):
        self._ensure_access_token()
        headers = {
            'Authorization': f'Bearer {self._access_token}',
            'Content-Type': 'application/json'
        }

        if not url.startswith('http'):
            url = f'https://api.spotify.com/v1/{url.lstrip("/")}'

        retries = 3
        while retries > 0:
            start_time = time.time()
            response = requests.request(method, url, headers=headers, params=params, json=json_data)
            end_time = time.time()

            logger.debug('Spotify API request %s %s: %.3f s, status %d', method, url, end_time - start_time, response.status_code)

            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 1))
                if retry_after > 5:
                    raise Exception(f"Spotify API rate-limited (429) with Retry-After={retry_after}s. Aborting to avoid hanging.")
                logger.warning(f"Spotify API rate-limited (429). Sleeping for {retry_after}s...")
                time.sleep(retry_after)
                retries -= 1
                continue

            if response.status_code in [500, 502, 503, 504]:
                retry_after = response.headers.get('Retry-After')
                if retry_after:
                    try:
                        retry_seconds = int(retry_after)
                        if retry_seconds > 5:
                            raise Exception(f"Spotify API Server Error ({response.status_code}) with Retry-After={retry_after}s. Aborting.")
                        time.sleep(retry_seconds)
                    except ValueError:
                        time.sleep(1)
                else:
                    time.sleep(1)
                retries -= 1
                continue

            if response.status_code not in [200, 201, 202, 204]:
                raise Exception(f"Spotify API request failed: {response.status_code} {response.text}")

            if response.status_code == 204 or not response.text.strip():
                return None

            return response.json()

        raise Exception("Spotify API requests failed after retries due to server errors/rate limits.")

    def _batch_result(self, url, params=None):
        results = self._api_request('GET', url, params=params)
        items = results['items']
        while results.get('next'):
            results = self._api_request('GET', results['next'])
            items += results['items']
        return items

    def invalidate_cache(self):
        self._cache = cache.Cache()

    def get_user_id(self):
        return self._api_request('GET', 'me')['id']

    def get_playlists(self):
        def body():
            results = self._batch_result('me/playlists')
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
        user_id = self.get_user_id()
        json_data = {
            'name': playlist_name,
            'public': False,
            'collaborative': False
        }
        self._api_request('POST', f'users/{user_id}/playlists', json_data=json_data)
        self._cache.invalidate('playlists')

    def delete_playlist(self, playlist_name):
        playlist_id = self.get_playlist_id(playlist_name)
        self._api_request('DELETE', f'playlists/{playlist_id}/followers')
        self._cache.invalidate('playlists')

    def _get_playlist_id_if_necessary(self, playlist_name_or_id):
        if is_spotify_id(playlist_name_or_id):
            return playlist_name_or_id
        return self.get_playlist_id(playlist_name_or_id)

    def get_playlist_tracks(self, playlist_name_or_id):
        playlist_id = self._get_playlist_id_if_necessary(playlist_name_or_id)
        def body():
            results = self._batch_result(f'playlists/{playlist_id}/tracks')
            results = _postprocess_tracks(results)
            df = pd.DataFrame.from_records(results)
            if not df.empty:
                df = df.set_index('spotify_id', drop=False)
            return df
        return self._cache.look_up_or_get(body, _TTL, 'playlist_tracks', playlist_id)

    def get_liked_tracks(self):
        def body():
            results = self._batch_result('me/tracks')
            results = _postprocess_tracks(results)
            df = pd.DataFrame.from_records(results)
            if not df.empty:
                df = df.set_index('spotify_id', drop=False)
            return df
        return self._cache.look_up_or_get(body, _TTL, 'liked_tracks')

    def get_artist_albums(self, artist_id):
        results = self._batch_result(f'artists/{artist_id}/albums')
        results = _postprocess_albums(results)
        df = pd.DataFrame.from_records(results)
        if not df.empty:
            df = df.set_index('album_id', drop=False)
        return df

    def get_album_tracks(self, album_id):
        album_info = self._api_request('GET', f'albums/{album_id}')
        album_entry = project(album_info, _ALBUM_COLUMNS)
        results = self._batch_result(f'albums/{album_id}/tracks')
        projection = project(
            results,
            {
                'spotify_id': 'id',
                'name': None,
                'artist_ids': lambda t: '|'.join([artist['id'] for artist in t['artists']]),
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
        result = self._api_request('GET', 'me/player/recently-played', params={'limit': 50})
        results = result['items']
        results = _postprocess_tracks(results)
        df = pd.DataFrame.from_records(results)
        if not df.empty:
            df = df.set_index('spotify_id', drop=False)
        return df

    def get_tracks_by_id(self, ids, raw=False):
        results = []
        start = 0
        while start < len(ids):
            end = min(start + _MAX_ITEMS_PER_REQUEST, len(ids))
            chunk = list(ids[start:end])
            res = self._api_request('GET', 'tracks', params={'ids': ','.join(chunk)})
            results += res['tracks']
            start = end
            
        if raw:
            return results
            
        results = _postprocess_tracks(results)
        df = pd.DataFrame.from_records(results)
        if not df.empty:
            df = df.set_index('spotify_id', drop=False)
        return df

    def search(self, search_string, limit=10, raw=False):
        results = self._api_request('GET', 'search', params={'q': search_string, 'limit': limit, 'type': 'track'})
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
            
        start = 0
        while start < len(tracks):
            end = min(start + _MAX_ITEMS_PER_REQUEST, len(tracks))
            chunk = list(tracks[start:end])
            uris = [f"spotify:track:{tid}" for tid in chunk]
            self._api_request('POST', f'playlists/{playlist_id}/tracks', json_data={'uris': uris})
            start = end
            
        print("Added %d new tracks to playlist '%s'" % (len(tracks), playlist_name_or_id))
        self._cache.invalidate('playlist_tracks', playlist_id)

    def replace_tracks_in_playlist(self, playlist_name_or_id, tracks):
        playlist_id = self._get_playlist_id_if_necessary(playlist_name_or_id)
        if isinstance(tracks, pd.DataFrame):
            tracks = tracks.spotify_id
            
        if len(tracks) == 0:
            self._api_request('PUT', f'playlists/{playlist_id}/tracks', json_data={'uris': []})
        else:
            end = min(_MAX_ITEMS_PER_REQUEST, len(tracks))
            first_chunk = list(tracks[0:end])
            uris = [f"spotify:track:{tid}" for tid in first_chunk]
            self._api_request('PUT', f'playlists/{playlist_id}/tracks', json_data={'uris': uris})
            
            start = end
            while start < len(tracks):
                end = min(start + _MAX_ITEMS_PER_REQUEST, len(tracks))
                chunk = list(tracks[start:end])
                uris = [f"spotify:track:{tid}" for tid in chunk]
                self._api_request('POST', f'playlists/{playlist_id}/tracks', json_data={'uris': uris})
                start = end
                
        print(f"Replaced contents of Spotify playlist '{playlist_name_or_id}' with {len(tracks)} tracks")
        self._cache.invalidate('playlist_tracks', playlist_id)

    def remove_tracks_from_playlist(self, playlist_name_or_id, tracks):
        playlist_id = self._get_playlist_id_if_necessary(playlist_name_or_id)
        if isinstance(tracks, pd.DataFrame):
            tracks = tracks.spotify_id
            
        start = 0
        while start < len(tracks):
            end = min(start + _MAX_ITEMS_PER_REQUEST, len(tracks))
            chunk = list(tracks[start:end])
            track_objects = [{'uri': f"spotify:track:{tid}"} for tid in chunk]
            self._api_request('DELETE', f'playlists/{playlist_id}/tracks', json_data={'tracks': track_objects})
            start = end
            
        self._cache.invalidate('playlist_tracks', playlist_id)

    def add_liked_tracks(self, tracks):
        if isinstance(tracks, pd.DataFrame):
            tracks = tracks.spotify_id
        if not isinstance(tracks, pd.Index):
            tracks = pd.Index(tracks)
            
        already_liked_tracks = self.get_liked_tracks().index
        new_tracks = tracks.difference(already_liked_tracks, sort=False)
        
        if len(new_tracks) < len(tracks):
            print('Ignoring %d already liked tracks' % (len(tracks) - len(new_tracks)))
            
        start = 0
        while start < len(new_tracks):
            end = min(start + _MAX_ITEMS_PER_REQUEST, len(new_tracks))
            chunk = list(new_tracks[start:end])
            self._api_request('PUT', 'me/tracks', json_data={'ids': chunk})
            start = end
            
        print('Added %d liked tracks' % len(new_tracks))
        self._cache.invalidate('liked_tracks')

    def remove_liked_tracks(self, tracks):
        if isinstance(tracks, pd.DataFrame):
            tracks = tracks.spotify_id
            
        start = 0
        while start < len(tracks):
            end = min(start + _MAX_ITEMS_PER_REQUEST, len(tracks))
            chunk = list(tracks[start:end])
            self._api_request('DELETE', 'me/tracks', json_data={'ids': chunk})
            start = end
            
        self._cache.invalidate('liked_tracks')
