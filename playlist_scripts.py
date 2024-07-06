
import re

from djlib_config import *
from utils import *

from local_scripts import *


_playlists = {
    'ALL 120':
        lambda track: track.BPM >= 110,

    'A 120':
        lambda track: track.BPM >= 110 and track.Class[0] == 'A',

    'AB 120':
        lambda track: track.BPM >= 110 and track.Class[0] in ['A', 'B'],

    'CX 120':
        lambda track: track.BPM >= 110 and track.Class[0] in ['C', 'X'],

    'ALL 100':
        lambda track: track.BPM <= 110,

    'A 100':
        lambda track: track.BPM <= 110 and track.Class[0] == 'A',

    'AB 100':
        lambda track: track.BPM <= 110 and track.Class[0] in ['A', 'B'],

    'CX 100':
        lambda track: track.BPM <= 110 and track.Class[0] in ['C', 'X'],

    'recent':
        lambda track: track['Date Added'] >= pd.Timestamp.now() - pd.Timedelta(60, 'days')
}

def djlib_values_sanity_check():
    djlib_tracks = docs['djlib'].read()

    errors = 0

    bad_class_filter = djlib_tracks.apply(
        lambda track: not re.match(r'[ABCX][1-5]?', track.Class),
        axis=1
    )
    bad_class_tracks = djlib_tracks.loc[bad_class_filter]

    if len(bad_class_tracks) > 0:
        errors += 1

        print('%d djilb tracks have a malformed Class field' % len(bad_class_tracks))
        pretty_print_tracks(bad_class_tracks, indent=' '*4, enum=True)

    return (errors == 0)

def build_playlist(name,
                   condition=None,
                   rekordbox=False,
                   spotify=False,
                   rekordbox_folder=['managed'],
                   rekordbox_prefix='',
                   rekordbox_overwrite=True,
                   spotify_prefix='DJ ',
                   spotify_overwrite=True):
    global _playlists

    if condition is None:
        condition = _playlists[name]

    djlib_tracks = docs['djlib'].read()

    tracks_filter = djlib_tracks.apply(condition, axis=1)

    djlib_playlist = djlib_tracks.loc[tracks_filter]

    print("Playlist '%s': %d tracks" % (name, len(djlib_playlist)))
    pretty_print_tracks(djlib_playlist, indent=' '*4, enum=True)

    if rekordbox:
        rekordbox_name = rekordbox_prefix + name
        if rekordbox_folder is None:
            rekordbox_full_name = [rekordbox_name]
        elif isinstance(rekordbox_folder, str):
            rekordbox_full_name = [rekordbox_folder, rekordbox_name]
        elif isinstance(rekordbox_folder, list):
            rekordbox_full_name = rekordbox_folder + [rekordbox_name]
        else:
            raise ValueError(rekordbox_folder)

        print()
        print('Creating Rekordbox playlist %s' % rekordbox_full_name)
        rekordbox.create_playlist(rekordbox_full_name, djlib_playlist, overwrite=rekordbox_overwrite)
        rekordbox.write()

    if spotify:
        rekordbox_to_spotify_mapping = docs['rekordbox_to_spotify'].read()

        # remove empty mappings
        rekordbox_to_spotify_mapping = rekordbox_to_spotify_mapping.loc[
            ~pd.isna(rekordbox_to_spotify_mapping.spotify_id)
        ]

        djlib_playlist_with_spotify = djlib_playlist.merge(
            rekordbox_to_spotify_mapping,
            how='inner',
            left_index=True,
            right_index=True
        )

        if len(djlib_playlist_with_spotify) < len(djlib_playlist):
            print('%d tracks are missing Spotify mappings; omitting.' % (
                len(djlib_playlist) - len(djlib_playlist_with_spotify)
            ))

        spotify_playlist_name = spotify_prefix + name

        spotify_playlists = spotify.get_playlists()

        if spotify_playlist_name in spotify_playlists.index:
            spotify_playlist_id = spotify_playlists.at[spotify_playlist_name, 'id']

            if not spotify_overwrite:
                raise Exception("Spotify playlist '%s' already exists" % spotify_playlist_name)

            spotify_playlist_tracks = spotify.get_playlist_tracks(spotify_playlist_id)

            spotify_ids_to_remove = spotify_playlist_tracks.index.difference(
                djlib_playlist_with_spotify.spotify_id,
                sort=False
            )

            spotify_ids_to_add = pd.Index(djlib_playlist_with_spotify.spotify_id).difference(
                spotify_playlist_tracks.index,
                sort=False
            )

            print("Spotify playlist '%s' exists with %d tracks; removing %d tracks, adding %d tracks" % (
                spotify_playlist_name,
                len(spotify_playlist_tracks),
                len(spotify_ids_to_remove),
                len(spotify_ids_to_add)
            ))

            spotify.remove_tracks_from_playlist(spotify_playlist_id, spotify_ids_to_remove)
            spotify.add_tracks_to_playlist(spotify_playlist_id, spotify_ids_to_add)

        else:
            print("Creating Spotify playlist '%s' with %d tracks" % (
                spotify_playlist_name,
                len(djlib_playlist_with_spotify)
            ))

            spotify.add_playlist(spotify_playlist_name)
            spotify.add_tracks_to_playlist(djlib_playlist_with_spotify.spotify_id)

    return
