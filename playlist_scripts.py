
from djlib_config import *
from utils import *

_playlists = {
    'ALL 120':
        lambda track: track.BPM >= 110,

    'A 120':
        lambda track: track.BPM >= 110 and track.Class[0] == 'A',

    'AB 120':
        lambda track: track.BPM >= 110 and track.Class[0] in ['A', 'B'],

    'ALL 100':
        lambda track: track.BPM <= 110,

    'A 100':
        lambda track: track.BPM <= 110 and track.Class[0] == 'A',

    'AB 100':
        lambda track: track.BPM <= 110 and track.Class[0] in ['A', 'B'],

    'recent':
        lambda track: track['Date Added'] >= pd.Timestamp.utcnow() - pd.Timedelta(60, 'days') and track.Class[0] in ['A', 'B'],

    'progressive':
        lambda track: 'PROGRESSIVE' in map(lambda x: x.upper(), track.Flavors)
}


def build_playlist(name,
                   condition=None,
                   print_tracks=True,
                   do_rekordbox=False,
                   do_spotify=False,
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
    if print_tracks:
        pretty_print_tracks(djlib_playlist, indent=' '*4, enum=True)

    if do_rekordbox:
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

    if do_spotify:
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

            if len(spotify_ids_to_remove) > 0:
                spotify.remove_tracks_from_playlist(spotify_playlist_id, spotify_ids_to_remove)
            if len(spotify_ids_to_add) > 0:
                spotify.add_tracks_to_playlist(spotify_playlist_id, spotify_ids_to_add)

        else:
            print("Creating Spotify playlist '%s' with %d tracks" % (
                spotify_playlist_name,
                len(djlib_playlist_with_spotify)
            ))

            spotify.create_playlist(spotify_playlist_name)
            spotify.add_tracks_to_playlist(spotify_playlist_name, djlib_playlist_with_spotify.spotify_id)

    print()

    return


def playlist_maintenance(do_rekordbox=True, do_spotify=True):
    # build Main Library on Spotify; it already exists in Rekordbox
    build_playlist(
        'Main Library',
        condition=lambda x: True,
        print_tracks=False,
        do_rekordbox=False,
        do_spotify=True,
        spotify_overwrite=True
    )

    # build the class playlists
    for playlist, condition in _playlists.items():
        build_playlist(
            playlist,
            condition=condition,
            print_tracks=False,
            do_rekordbox=do_rekordbox,
            do_spotify=do_spotify,
            rekordbox_overwrite=True,
            spotify_overwrite=True
        )

    return
