
from djlib_config import *
from utils import *
from playlist_scripts import *

def retire_playlists_field():
    djlib_tracks = docs['djlib'].read()

    djlib_tracks.Danceable = djlib_tracks.apply(
        lambda track: has_value(track.Playlists, 'knocks', 'exuberance'),
        axis=1
    )

    djlib_tracks.Ambient = djlib_tracks.apply(
        lambda track: has_value(track.Playlists, 'feels'),
        axis=1
    )

    djlib_tracks.Song = djlib_tracks.apply(
        lambda track: has_value(track.Playlists, 'songs'),
        axis=1
    )

    choice = get_user_choice('Write djlib?')
    if choice == 'yes':
        docs['djlib'].write(djlib_tracks)

    return

