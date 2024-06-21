import os
import re
import spotipy
from spotipy.oauth2 import SpotifyOAuth

_MAX_TRACKS_PER_REQUEST = 100

_SCOPES = [
    # 'user-library-read',
    'user-library-modify',
    # 'playlist-read-private',
    # 'playlist-read-collaborative',
    'playlist-modify-private',
    'playlist-modify-public'
]

class SpotifyInterface:
    def __init__(self, config):
        self._client_id = config['client_id']
        self._client_secret = config['client_secret']
        self._redirect_uri = config['redirect_uri']
        self._cached_token_file = config['cached_token_file']

        # the connection will be initialized when it's first used
        self._connection = None

    def _init_connection(self):
        if self._connection is not None:
            return

        scope = ','.join(_SCOPES)

        self._connection = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope,
                                                            cache_path=self._cached_token_file,
                                                            client_id=self._client_id,
                                                            client_secret=self._client_secret,
                                                            redirect_uri=self._redirect_uri))

        return



