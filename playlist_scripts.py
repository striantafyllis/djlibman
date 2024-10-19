
from djlib_config import *
from utils import *

def is_class(track, *classes):
    for clss in classes:
        if track.Class.startswith(clss):
            return True

    return False

def has_value(list_field, *values):
    values = [v.upper() for v in values]

    for list_el in list_field:
        if list_el.upper() in values:
            return True

    return False


_playlist_conditions = {
    'ALL 120':
        lambda track: track.BPM >= 113,

    'A 120':
        lambda track: track.BPM >= 113 and is_class(track, 'A'),

    'AB 120':
        lambda track: track.BPM >= 113 and is_class(track, 'A', 'B'),

    'ALL 100':
        lambda track: track.BPM < 113,

    'A 100':
        lambda track: track.BPM < 113 and is_class(track, 'A'),

    'AB 100':
        lambda track: track.BPM < 113 and is_class(track, 'A', 'B'),

    'recent':
        lambda track: track['Date Added'] >= pd.Timestamp.utcnow() - pd.Timedelta(60, 'days') and is_class(track, 'A', 'B'),

    'progressive':
        lambda track: has_value(track.Flavors, 'Progressive') and has_value(track.Playlists, 'knocks') and is_class(track, 'A', 'B'),

    'afro':
        lambda track: has_value(track.Flavors, 'Afro') and has_value(track.Playlists, 'knocks') and is_class(track, 'A', 'B'),

    'middle eastern':
        lambda track: has_value(track.Flavors, 'Middle Eastern', 'Balkan', 'North Med') and has_value(track.Playlists, 'knocks') and is_class(track, 'A', 'B'),

    'latin':
        lambda track: has_value(track.Flavors, 'Latin') and has_value(track.Playlists, 'knocks') and is_class(track, 'A', 'B'),

    'feels':
        lambda track: has_value(track.Playlists, 'feels') and is_class(track, 'A', 'B')
}

def create_playlist_from_condition(
    name,
    condition=None,
    print_tracks=True,
    do_rekordbox=False,
    do_spotify=False,
    rekordbox_folder=['managed'],
    rekordbox_prefix='',
    rekordbox_overwrite=True,
    spotify_prefix='DJ ',
    spotify_overwrite=True):

    global _playlist_conditions

    if condition is None:
        condition = _playlist_conditions[name]

    djlib_tracks = docs['djlib'].read()

    tracks_filter = djlib_tracks.apply(condition, axis=1)

    track_ids_df = djlib_tracks.loc[tracks_filter]

    create_playlist(track_ids_df,
                    print_tracks,
                    do_rekordbox,
                    do_spotify,
                    rekordbox_folder,
                    rekordbox_prefix,
                    rekordbox_overwrite,
                    spotify_prefix,
                    spotify_overwrite)

    return


def create_playlist(
        name,
        track_ids_df,
        print_tracks=True,
        do_rekordbox=False,
        do_spotify=False,
        rekordbox_folder=['managed'],
        rekordbox_prefix='',
        rekordbox_overwrite=True,
        spotify_prefix='DJ ',
        spotify_overwrite=True):

    print("Playlist '%s': %d tracks" % (name, len(track_ids_df)))
    if print_tracks:
        pretty_print_tracks(track_ids_df, indent=' ' * 4, enum=True)

    if do_rekordbox:
        create_rekordbox_playlist(
            name,
            track_ids_df,
            print_tracks=False,
            folder=rekordbox_folder,
            prefix=rekordbox_prefix,
            overwrite=rekordbox_overwrite
        )

    if do_spotify:
        create_spotify_playlist(
            name,
            track_ids_df,
            print_tracks=False,
            spotify_prefix=spotify_prefix,
            spotify_overwrite=spotify_overwrite
        )

    print()

    return


def create_rekordbox_playlist(
    name,
    track_ids_df,
    print_tracks=True,
    folder=['managed'],
    prefix='',
    overwrite=True):

    rekordbox_name = prefix + name
    if folder is None:
        rekordbox_full_name = [rekordbox_name]
    elif isinstance(folder, str):
        rekordbox_full_name = [folder, rekordbox_name]
    elif isinstance(folder, list):
        rekordbox_full_name = folder + [rekordbox_name]
    else:
        raise ValueError(folder)

    print(f'Creating Rekordbox playlist {rekordbox_full_name}: {len(track_ids_df)} tracks')
    if print_tracks:
        pretty_print_tracks(track_ids_df, indent=' ' * 4, enum=True)

    rekordbox.create_playlist(rekordbox_full_name, track_ids_df, overwrite=overwrite)
    rekordbox.write()

    return


def create_spotify_playlist(
        name,
        track_ids_df,
        print_tracks=True,
        spotify_prefix='DJ ',
        spotify_overwrite=True
):
    rekordbox_to_spotify_mapping = docs['rekordbox_to_spotify'].read()

    # remove empty mappings
    rekordbox_to_spotify_mapping = rekordbox_to_spotify_mapping.loc[
        ~pd.isna(rekordbox_to_spotify_mapping.spotify_id)
    ]

    spotify_ids_df = track_ids_df.merge(
        rekordbox_to_spotify_mapping,
        how='inner',
        left_index=True,
        right_index=True
    )

    if len(spotify_ids_df) < len(track_ids_df):
        print('%d tracks are missing Spotify mappings; omitting.' % (
                len(track_ids_df) - len(spotify_ids_df)
        ))

    spotify_playlist_name = spotify_prefix + name

    print(f'Creating Spotify playlist {spotify_playlist_name}: {len(spotify_ids_df)} tracks')
    if print_tracks:
        pretty_print_tracks(track_ids_df, indent=' ' * 4, enum=True)

    spotify_playlists = spotify.get_playlists()

    if spotify_playlist_name in spotify_playlists.index:
        spotify_playlist_id = spotify_playlists.at[spotify_playlist_name, 'id']

        if not spotify_overwrite:
            raise Exception("Spotify playlist '%s' already exists" % spotify_playlist_name)

        spotify_playlist_tracks = spotify.get_playlist_tracks(spotify_playlist_id)

        spotify_ids_to_remove = spotify_playlist_tracks.index.difference(
            spotify_ids_df.spotify_id,
            sort=False
        )

        spotify_ids_to_add = pd.Index(spotify_ids_df.spotify_id).difference(
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
            len(spotify_ids_df)
        ))

        spotify.create_playlist(spotify_playlist_name)
        spotify.add_tracks_to_playlist(spotify_playlist_name, spotify_ids_df.spotify_id)

    return


def playlist_maintenance(do_rekordbox=True, do_spotify=True):
    # build Main Library on Spotify; it already exists in Rekordbox
    create_playlist_from_condition(
        'Main Library',
        condition=lambda x: True,
        print_tracks=False,
        do_rekordbox=False,
        do_spotify=True,
        spotify_overwrite=True
    )

    # build the class playlists
    for playlist, condition in _playlist_conditions.items():
        create_playlist_from_condition(
            playlist,
            condition=condition,
            print_tracks=False,
            do_rekordbox=do_rekordbox,
            do_spotify=do_spotify,
            rekordbox_overwrite=True,
            spotify_overwrite=True
        )

    return
