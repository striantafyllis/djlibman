
from data_model import *

class StreamingService:
    def __init__(self):
        pass

    def name(self) -> str:
        """Returns the name of the service"""
        raise Exception('Not implemented')

    def search(self, track: Track) -> Tracklist:
        """Searches for a track; returns a list of tracks found"""
        raise Exception('Not implemented')

    def get_playlists(self) -> dict[str, str]:
        """Returns all the user's playlists in a uri->name dictionary"""
        raise Exception('Not implemented')

    def get_playlist_tracks(self, playlist_uri) -> Playlist:
        """Returns the tracks in a playlist as a list of (URI, string description) pairs"""
        raise Exception('Not implemented')

    def delete_playlist(self, playlist_uri: str) -> None:
        """Deletes a playlist"""
        raise Exception('Not implemented')

    def create_playlist(self, playlist_name: str) -> str:
        """Creates a playlist with the given name and returns its URI"""
        raise Exception('Not implemented')

    def add_tracks_to_playlist(self, playlist_uri: str, track_uris: list[str]) -> None:
        """Adds the specified tracks to the playlist"""
        raise Exception('Not implemented')

    def remove_tracks_from_playlist(self, playlist_uri: str, track_uris: list[str]) -> None:
        """Removes the specified tracks from the playlist"""
        raise Exception('Not implemented')
