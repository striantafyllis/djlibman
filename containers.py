

import pandas as pd
import numpy as np

from spyroslib import containers as ct
from spyroslib.containers import Container, Doc, Wrapper

import djlib_config

from local_util import *
from spotify_util import *

def translate_spotify_id_to_rekordbox(spotify_df: pd.DataFrame) -> pd.DataFrame:
    """Converts a dataframe indexed by spotify_id to one indexed by rekordbox_id"""
    # TODO this should really be in a different file and passed in as a function argument

    if spotify_df.index.name != 'spotify_id':
        raise ValueError('Expected a DF indexed by spotify_id')

    rekordbox_to_spotify_mapping = djlib_config.docs['rekordbox_to_spotify'].read()

    # remove empty mappings
    rekordbox_to_spotify_mapping = rekordbox_to_spotify_mapping.loc[
        ~pd.isna(rekordbox_to_spotify_mapping.spotify_id)
    ]

    spotify_df = spotify_df.merge(
        right=rekordbox_to_spotify_mapping,
        how='inner',
        left_index=True,
        right_on='spotify_id'
    )

    spotify_df = spotify_df.set_index(keys='rekordbox_id', drop=False)

    return spotify_df

def translate_rekordbox_id_to_spotify(rekordbox_df: pd.DataFrame) -> pd.DataFrame:
    if rekordbox_df.index.name != 'rekordbox_id':
        raise ValueError('Expected a DF indexed by rekordbox_id')

    rekordbox_to_spotify_mapping = djlib_config.docs['rekordbox_to_spotify'].read()

    # remove empty mappings
    rekordbox_to_spotify_mapping = rekordbox_to_spotify_mapping.loc[
        ~pd.isna(rekordbox_to_spotify_mapping.spotify_id)
    ]

    rekordbox_df = rekordbox_df.merge(
        right=rekordbox_to_spotify_mapping,
        how='inner',
        left_index=True,
        right_index=True
    )

    rekordbox_df = rekordbox_df.set_index(keys='spotify_id', drop=False)

    return rekordbox_df

def djlibman_id_translator_func(this_index_name, other_df):
    if this_index_name == 'spotify_id':
        if other_df.index.name == 'rekordbox_id':
            other_df = translate_rekordbox_id_to_spotify(other_df)
        else:
            raise ValueError(f"Unknown other index {other_df.index.name}")
    elif this_index_name == 'rekordbox_id':
        if other_df.index.name == 'spotify_id':
            other_df = translate_spotify_id_to_rekordbox(other_df)
        else:
            raise ValueError(f"Unknown other index {other_df.index.name}")
    else:
        raise ValueError(f"Unknown index {this_index_name}")

    return other_df

ct.set_default_id_translator_func(djlibman_id_translator_func)

ct.set_default_printer_func(lambda df: pretty_print_tracks(df, enum=True))


class SpotifyPlaylist(Container):
    def __init__(self, name: str, modify=True, create=False, overwrite=False):
        self._playlist_name = name
        super(SpotifyPlaylist, self).__init__(
            f"Spotify playlist {name}",
            modify=modify, create=create, overwrite=overwrite)
        return

    def _get_index_name(self):
        return 'spotify_id'

    def _check_existence(self):
        return djlib_config.spotify.playlist_exists(self._playlist_name)

    def _read(self, force=False):
        return djlib_config.spotify.get_playlist_tracks(self._playlist_name)

    def _write_back(self, df):
        if not self._exists:
            djlib_config.spotify.create_playlist(self._playlist_name)
        djlib_config.spotify.replace_tracks_in_playlist(self._playlist_name, df)
        return

class SpotifyLiked(Container):
    def __init__(self):
        super(SpotifyLiked, self).__init__('Spotify Liked Tracks', modify=True, create=False, overwrite=False)
        return

    def _get_index_name(self):
        return 'spotify_id'

    def _check_existence(self):
        return True

    def _read(self, force=False):
        return djlib_config.spotify.get_liked_tracks()

    def _write_back(self, df):
        liked_tracks = djlib_config.spotify.get_liked_tracks()

        tracks_to_add = df.index.difference(liked_tracks.index, sort=False)
        tracks_to_remove = liked_tracks.index.difference(df.index, sort=False)

        if len(tracks_to_add) > 0:
            print(f'Adding {len(tracks_to_add)} Spotify Liked Tracks')
            djlib_config.spotify.add_liked_tracks(tracks_to_add)

        if len(tracks_to_remove) > 0:
            print(f'Removing {len(tracks_to_remove)} Spotify Liked Tracks')
            djlib_config.spotify.remove_liked_tracks(tracks_to_remove)

        return


class RekordboxCollection(Container):
    def __init__(self):
        super(RekordboxCollection, self).__init__(
            name='Rekordbox Collection',
            create=False,
            modify=False,
            overwrite=False,
            prompt=False
        )
        return

    def _get_index_name(self):
        return 'rekordbox_id'

    def _check_existence(self):
        return True

    def _read(self, force=False):
        return djlib_config.rekordbox.get_collection()


class RekordboxPlaylist(Container):
    def __init__(self, name: str, modify=True, create=False, overwrite=False):
        self._playlist_name = name
        super(RekordboxPlaylist, self).__init__(
            f"Rekordbox playlist {name}",
            modify=modify, create=create, overwrite=overwrite)
        return

    def _get_index_name(self):
        return 'rekordbox_id'

    def _check_existence(self):
        return djlib_config.rekordbox.playlist_exists(self._playlist_name)

    def _read(self, force=False):
        return djlib_config.rekordbox.get_playlist_tracks(self._playlist_name)

    def _write_back(self, df, write_thru=True):
        djlib_config.rekordbox.create_playlist(self._playlist_name, df, overwrite=True)
        if write_thru:
            djlib_config.rekordbox.write()
        return

class Queue(Doc):
    """A special doc for the Spotify queue; it sets added_at to now() when adding tracks."""
    def __init__(
            self,
            name: str = 'queue', *,
            modify=True,
            create=True,
            overwrite=True,
            **kwargs):
        super(Queue, self).__init__(
            name=name,
            type='csv',
            modify=modify,
            create=create,
            overwrite=overwrite,
            index_name='spotify_id',
            header=0,
            datetime_columns=['release_date', 'added_at'],
            **kwargs
        )

    def _preprocess_before_append(self, df: pd.DataFrame):
        df = df.assign(added_at=pd.Timestamp.utcnow())
        return df


class ListeningHistory(Doc):
    """A special doc for the listening history. It supports filtering by signature along with ID,
       plus it prohibits some operations that don't make sense."""

    def __init__(
            self,
            name: str = 'listening_history'):
        super(ListeningHistory, self).__init__(
            name=name,
            modify=True,
            create=False,
            overwrite=False,
            index_name='spotify_id'
        )

        self._track_signatures = None

    def _rvalue_check(self, operation):
        raise ValueError(
            'ListeningHistory should not be added to or removed from other containers')

    def _preprocess_before_append(self, df: pd.DataFrame):
        df = df.assign(added_at=pd.Timestamp.utcnow())
        return df

    def append(self, other, prompt=None):
        super(ListeningHistory, self).append(other, prompt)

        self._track_signatures = None
        return

    def remove(self, other, prompt=None, force=False):
        if not force:
            raise ValueError('Why remove from listening history?')
        super(ListeningHistory, self).remove(other, prompt)

    def _ensure_track_signatures(self):
        self._ensure_df()

        self._track_signatures = pd.Index(
            self._df.apply(
                get_track_signature,
                axis=1
            )
        )

        return

    def filter(self, other: Container, prompt=None, silent=False):
        self._ensure_track_signatures()

        if not isinstance(other, Container):
            raise ValueError("'other' argument must be a container")

        other_df = other.get_df()

        if len(other_df) == 0:
            return

        if other_df.index.name != 'spotify_id':
            raise ValueError('Only Spotify tracks indexed by spotify_id can be filtered '
                             'through listening history')

        other_not_listened_index = other_df.index.difference(self._df.index, sort=False)
        filtered_through_spotify_id = len(other_df.index) - len(other_not_listened_index)

        if filtered_through_spotify_id != 0:
            other_df = other_df.loc[other_not_listened_index]

        # for some reason Pandas.apply doesn't work correctly if the dataframe is empty;
        # it returns an empty dataframe instead of a boolean array
        if len(other_df) > 0:
            other_not_listened_sigs = other_df.apply(
                lambda track: get_track_signature(track) not in self._track_signatures,
                axis=1
            )

            other_df_not_listened_sigs = other_df.loc[other_not_listened_sigs]

            filtered_through_track_sigs = len(other_df) - len(other_df_not_listened_sigs)

            if filtered_through_track_sigs != 0:
                other_df = other_df_not_listened_sigs
        else:
            filtered_through_track_sigs = 0

        filtered = filtered_through_spotify_id + filtered_through_track_sigs

        if filtered != 0:
            if not silent:
                filtered_tracks = other._df.loc[
                    other._df.index.difference(other_df.index, sort=False)
                ]

                print(f'{other.get_name()}: removing {filtered} tracks from listening '
                      f'history - {filtered_through_spotify_id} by spotify ID and '
                      f'{filtered_through_track_sigs} by track signatures')

                pretty_print_tracks(filtered_tracks)

            if other._should_prompt(prompt):
                choice = get_user_choice('Proceed?')
                if choice != 'yes':
                    return

            # not calling set_df() here because we don't want the overwrite
            # check and the ID reconciliation
            other._df = other_df
            other._changed = True

        elif not silent:
            print(f'{other.get_name()}: no tracks in listening history')

        return
