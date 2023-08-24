import sys
import os
import re
import spotipy
from spotipy.oauth2 import SpotifyOAuth

import google_sheet
import streaming_service

SPOTIPY_CLIENT_ID = os.environ['SPOTIPY_CLIENT_ID']
SPOTIPY_CLIENT_SECRET = os.environ['SPOTIPY_CLIENT_SECRET']
SPOTIPY_REDIRECT_URI = os.environ['SPOTIPY_REDIRECT_URI']

MAX_TRACKS_PER_REQUEST = 100

SCOPES = [
    # 'user-library-read',
    'user-library-modify',
    # 'playlist-read-private',
    # 'playlist-read-collaborative',
    'playlist-modify-private',
    'playlist-modify-public'
]

CACHED_TOKEN_FILE = 'spotify_cached_token.json'


class SpotifyService(streaming_service.StreamingService):
    def __init__(self):

        scope = ','.join(SCOPES)

        self.spotify = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope,
                                                            cache_path=CACHED_TOKEN_FILE,
                                                            client_id=SPOTIPY_CLIENT_ID,
                                                            client_secret=SPOTIPY_CLIENT_SECRET,
                                                            redirect_uri=SPOTIPY_REDIRECT_URI))

        return

    def name(self):
        return 'Spotify'

    def search(self, track: google_sheet.TrackInfo):
        query = ' '.join(track.artists)

        # preprocess the title to make it more likely that spotify will find it
        title = track.title

        title = re.sub(r'feat\.', '', title, flags=re.IGNORECASE)
        title = re.sub(r'\(?original mix\)?', '', title, flags=re.IGNORECASE)

        title = title.replace('(', ' ').replace(')', ' ')

        query += ' ' + title

        # print("        Spotify query: '" + query + "'")

        results = self.spotify.search(
            # there are more sophisticated ways to do search using field tags such as
            # track:, artist: etc., but it doesn't seem to work well, especially when
            # there are multiple artists. So it's best to just send an undifferentiated string.
            q = query,
            type = 'track'
        )

        return_val = []

        for track in results['tracks']['items']:
            artists = [artist['name'] for artist in track['artists']]
            title = track['name']
            uri = track['id']
            url = 'https://open.spotify.com/track/%s' % uri

            description = '%s \u2013 %s %s' % (', '.join(artists), title, url)

            return_val.append((uri, description))

        return return_val

    def get_playlists(self) -> dict[str, str]:
        user = self.spotify.current_user()['id']
        spotify_results = self.spotify.user_playlists(user=user)

        results = {}

        for spotify_playlist in spotify_results['items']:
            owner = spotify_playlist['owner']['id']
            name = spotify_playlist['name']
            uri = spotify_playlist['id']
            if owner != user:
                continue
            results[name] = uri

        return results

    def get_playlist_tracks(self, playlist_uri: str) -> list[tuple[str, str]]:
        return_val = []
        offset = 0

        while True:
            results = self.spotify.playlist_items(
                playlist_id=playlist_uri,
                limit=MAX_TRACKS_PER_REQUEST,
                offset=offset
            )

            for result in results['items']:
                track = result['track']

                artists = [artist['name'] for artist in track['artists']]
                title = track['name']
                uri = track['id']
                url = 'https://open.spotify.com/track/%s' % uri

                description = '%s \u2013 %s %s' % (', '.join(artists), title, url)

                return_val.append((uri, description))

            if len(results['items']) < MAX_TRACKS_PER_REQUEST:
                break

            offset += MAX_TRACKS_PER_REQUEST

        return return_val

    def delete_playlist(self, playlist_uri: str):
        self.spotify.current_user_unfollow_playlist(playlist_uri)
        return

    def create_playlist(self, playlist_name: str) -> str:
        user = self.spotify.current_user()['id']

        result = self.spotify.user_playlist_create(
            user=user,
            name=playlist_name,
            public=False,
            collaborative=False)

        return result['id']

    def add_tracks_to_playlist(self,
                               playlist_uri: str,
                               track_uris: list[str]):
        # Spotify allows at most 100 tracks per request...
        while len(track_uris) > 0:
            cur_track_uris = track_uris[:MAX_TRACKS_PER_REQUEST]
            self.spotify.playlist_add_items(playlist_id=playlist_uri, items=cur_track_uris)
            track_uris = track_uris[MAX_TRACKS_PER_REQUEST:]
        return

    def remove_tracks_from_playlist(self,
                                    playlist_uri: str,
                                    track_uris: list[str]) -> None:
        # Spotify allows at most 100 tracks per request...
        while len(track_uris) > 0:
            cur_track_uris = track_uris[:MAX_TRACKS_PER_REQUEST]
            self.spotify.playlist_remove_all_occurrences_of_items(playlist_id=playlist_uri, items=cur_track_uris)
            track_uris = track_uris[MAX_TRACKS_PER_REQUEST:]
        return

def main():
    service = SpotifyService()
    results = service.spotify.current_user_saved_tracks()
    for idx, item in enumerate(results['items']):
        track = item['track']
        print(idx, track['artists'][0]['name'], " – ", track['name'])

    playlists = service.get_playlists()

    return 0

if __name__ == '__main__':
    sys.exit(main())