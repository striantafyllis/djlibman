

import spotipy
import sys
import os
from spotipy.oauth2 import SpotifyOAuth

SPOTIPY_CLIENT_ID = os.environ['SPOTIPY_CLIENT_ID']
SPOTIPY_CLIENT_SECRET = os.environ['SPOTIPY_CLIENT_SECRET']
SPOTIPY_REDIRECT_URI = os.environ['SPOTIPY_REDIRECT_URI']

scope = "user-library-read"

# cache_handler = spotipy.CacheFileHandler(cache_path='spotify_cached_token.json')

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope,
                                               cache_path='spotify_cached_token.json',
                                               client_id=SPOTIPY_CLIENT_ID,
                                               client_secret=SPOTIPY_CLIENT_SECRET,
                                               redirect_uri=SPOTIPY_REDIRECT_URI))

results = sp.current_user_saved_tracks()
for idx, item in enumerate(results['items']):
    track = item['track']
    print(idx, track['artists'][0]['name'], " – ", track['name'])

