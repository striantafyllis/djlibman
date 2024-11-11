
from djlib_config import *
from utils import *

def is_class(track, *classes):
    for clss in classes:
        if track.Class.startswith(clss):
            return True

    return False

def has_value(list_field, values):
    if isinstance(values, str):
        values = [values.upper()]
    else:
        values = [v.upper() for v in values]

    for list_el in list_field:
        if list_el.upper() in values:
            return True

    return False

def build_condition(
        must_be_danceable=True,
        must_be_ambient=False,
        must_be_song=False,
        allow_uptempo=True,
        allow_downtempo=False,
        allow_B=True,
        allow_CX=False,
        flavors=None
):
    def _condition(track):
        if must_be_danceable:
            if not track.Danceable:
                return False
        if must_be_ambient:
            if not track.Ambient:
                return False
        if must_be_song:
            if not track.Song:
                return False
        if not allow_uptempo:
            if track.BPM > DOWNTEMPO_CUTOFF:
                return False
        if not allow_downtempo:
            if track.BPM <= DOWNTEMPO_CUTOFF:
                return False
        if not allow_B:
            if is_class(track, 'B'):
                return False
        if not allow_CX:
            if not is_class(track, 'A', 'B'):
                return False
        if flavors is not None:
            if not has_value(track.Flavors, flavors):
                return False
        return True

    return _condition


DOWNTEMPO_CUTOFF = 112


_playlist_conditions = {
    'Danceable A':
        build_condition(allow_B=False),

    'Danceable AB':
        build_condition(),

    'Danceable Downtempo A':
        build_condition(allow_uptempo=False, allow_downtempo=True, allow_B=False),

    'Danceable Downtempo AB':
        build_condition(allow_uptempo=False, allow_downtempo=True),

    'Ambient':
        build_condition(must_be_danceable=False, must_be_ambient=True),

    'Recent':
        lambda track: track['Date Added'] >= pd.Timestamp.utcnow() - pd.Timedelta(60, 'days') and is_class(track, 'A', 'B'),

    'Progressive':
        build_condition(flavors='Progressive'),

    'Afro':
        build_condition(flavors='Afro'),

    'Middle Eastern':
        build_condition(flavors=['Middle Eastern', 'Balkan', 'North Med']),

    'Latin':
        build_condition(flavors='Latin')
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

    create_playlist(
        name,
        track_ids_df,
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

    if isinstance(track_ids_df, pd.DataFrame):
        spotify_ids_df = track_ids_df.merge(
            rekordbox_to_spotify_mapping,
            how='inner',
            left_index=True,
            right_index=True
        )
    elif isinstance(track_ids_df, pd.Index):
        spotify_ids_df = rekordbox_to_spotify_mapping.loc[track_ids_df]
    else:
        raise ValueError(f'Invalid track_ids_df type: {type(track_ids_df)}')

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

def create_spotify_playlist_from_rekordbox(
        spotify_name,
        rekordbox_name,
        print_tracks=True,
        spotify_prefix='DJ ',
        spotify_overwrite=True
):
    track_ids_df = rekordbox.get_playlist_tracks(rekordbox_name)

    create_spotify_playlist(
        spotify_name,
        track_ids_df,
        print_tracks=print_tracks,
        spotify_prefix=spotify_prefix,
        spotify_overwrite=spotify_overwrite
    )

    return

def create_rekordbox_playlist_from_spotify(
        rekordbox_name,
        spotify_name,
        folder=['managed'],
        prefix='',
        overwrite=True):

    spotify_tracks = spotify.get_playlist_tracks(spotify_name)

    rekordbox_to_spotify_mapping = docs['rekordbox_to_spotify'].read()

    rekordbox_ids_df = rekordbox_to_spotify_mapping.merge(
        spotify_tracks,
        how='inner',
        left_on='spotify_id',
        right_index=True
    )

    if len(rekordbox_ids_df) < len(spotify_tracks):
        print(f'*** WARNING! *** {len(spotify_tracks) - len(rekordbox_ids_df)} Spotify tracks are missing Rekordbox IDs! Omitting.')
        choice = get_user_choice('Continue?')
        if choice != 'yes':
            return

    create_rekordbox_playlist(
        rekordbox_name,
        rekordbox_ids_df,
        folder=folder,
        prefix=prefix,
        overwrite=overwrite
    )

    return


def playlist_maintenance(do_rekordbox=True, do_spotify=True):
    # build Main Library on Spotify; it already exists in Rekordbox
    if do_spotify:
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
