
"""
Wraps our various dataframe-like data objects - docs. Spotify playlists, Rekordbox playlists etc.
- into objects that you can manipulate and write back. These data objects can be added to each other,
subtracted from each other etc.
"""

import numpy as np
import pandas as pd

from general_utils import *
# from djlib_config import *

import djlib_config

class Container(object):
    """Root of the hierarchy; abstract class"""
    def __init__(self, name: str):
        self._name = name
        self._df = None
        return

    def _read(self) -> pd.DataFrame:
        raise NotImplementedError()

    def _write_back(self, df: pd.DataFrame) -> None:
        raise NotImplementedError()

    def _ensure_df(self):
        if self._df is None:
            self._df = self._read()
        return

    def get_name(self) -> str:
        return self._name

    def get_df(self) -> pd.DataFrame:
        self._ensure_df()
        return self._df

    def write(self) -> None:
        if self._df is None:
            raise Exception('Nothing to write')

        self._write_back(self._df)
        return

    def _reconcile_ids(self, other_df):
        self._ensure_df()

        if self._df.index.name == other_df.index.name:
            return

        if self._df.index.name in ['id', 'spotify_id']:
            if other_df.index.name in ['id', 'spotify_id']:
                pass
            elif other_df.index.name in ['TrackID', 'rekordbox_id']:
                rekordbox_to_spotify_mapping = djlib_config.docs['rekordbox_to_spotify'].read()

                # remove empty mappings
                rekordbox_to_spotify_mapping = rekordbox_to_spotify_mapping.loc[
                    ~pd.isna(rekordbox_to_spotify_mapping.spotify_id)
                ]

                if self._df.index.name != 'spotify_id':
                    rekordbox_to_spotify_mapping = rekordbox_to_spotify_mapping.rename(
                        columns={'spotify_id': self._df.index.name})

                other_df = other_df.merge(
                    right=rekordbox_to_spotify_mapping,
                    how='inner',
                    left_index=True,
                    right_index=True
                )

                other_df = other_df.set_index(keys=self._df.index.name, drop=False)
            else:
                raise ValueError(f"Unknown other index {other_df.index.name}")
        elif self._df.index.name in ['TrackID', 'rekordbox_id']:
            if other_df.index.name in ['TrackID', 'rekordbox_id']:
                pass
            elif other_df.index.name in ['id', 'spotify_id']:
                rekordbox_to_spotify_mapping = docs['rekordbox_to_spotify'].read()

                # remove empty mappings
                rekordbox_to_spotify_mapping = rekordbox_to_spotify_mapping.loc[
                    ~pd.isna(rekordbox_to_spotify_mapping.spotify_id)
                ]

                if self._df.index.name != 'rekordbox_id':
                    rekordbox_to_spotify_mapping = rekordbox_to_spotify_mapping.rename(
                        columns={'rekordbox_id': self._df.index.name})

                other_df = other_df.merge(
                    right=rekordbox_to_spotify_mapping,
                    how='inner',
                    left_index=True,
                    right_on='spotify_id'
                )

                other_df = other_df.set_index(keys=self._df.index.name, drop=False)
            else:
                raise ValueError(f"Unknown other index {other_df.index.name}")
        else:
            raise ValueError(f"Unknown index {self._df.index.name}")

        return other_df

    def _reconcile_columns(self, other_df):
        for column in self._df.columns:
            if column not in other_df.columns:
                other_df[column] = np.nan

        # this takes care of extra columns, columns in different order etc.
        other_df = other_df[self._df.columns]

        return other_df

    def deduplicate(self) -> None:
        self._ensure_df()

        print(f'Deduplicating {self.get_name()}...')

        dup_pos = dataframe_duplicate_index_labels(self._df)
        if len(dup_pos) > 0:
            print(f'{self.get_name()} has {len(dup_pos)} duplicate entries!')
            choice = get_user_choice('Remove?')
            if choice == 'yes':
                self._df = dataframe_drop_rows_at_positions(self._df, dup_pos)

        return

    def _get_set_operation_result(self, other, operation):
        self._ensure_df()

        if isinstance(other, pd.Index):
            other_idx = other
        else:
            if isinstance(other, Container):
                other_df = other.get_df()
            elif isinstance(other, pd.DataFrame):
                other_df = other
            else:
                raise ValueError(f'Invalid argument type: {type(other)}')

            other_df = self._reconcile_ids(other_df)

            other_idx = other_df.index

        # continue here

        if operation == 'intersection':
            new_idx = self._df.index.intersection(other_idx, sort=False)
        elif operation == 'difference':
            new_idx = self._df.index.difference(other_idx, sort=False)

        return self.df.loc[new_idx]

    def get_intersection(self, other):
        return self._get_set_operation_result(other, 'intersection')

    def get_difference(self, other):
        return self._get_set_operation_result(other, 'difference')

    def append(self, other) -> None:
        self._ensure_df()

        if isinstance(other, Container):
            other_df = other.get_df()
        elif isinstance(other, pd.DataFrame):
            other_df = other
        else:
            raise ValueError(f'Invalid argument type: {type(other)}')

        if len(other_df) == 0:
            return

        other_df = self._reconcile_ids(other_df)
        other_df = self._reconcile_columns(other_df)

        other_unique = dataframe_ensure_unique_index(other_df)

        num_dups = len(other_df) - len(other_unique)

        if len(self._df) == 0:
            self._df = other_unique
        else:
            # remove entries that are already there
            genuinely_new_entries_idx = other_unique.index.difference(self._df.index, sort=False)

            genuinely_new_entries = other_unique.loc[genuinely_new_entries_idx]

            num_already_present = len(other_unique) - len(genuinely_new_entries)
            num_added = len(genuinely_new_entries)

            new_df = pd.concat([self._df, genuinely_new_entries])

        status_str = f'{self.get_name()}: adding {other.get_name()}:'
        if num_added == 0:
            status_str += ' nothing to add'
        else:
            status_str += f' adding {num_added} entries'

        if num_dups != 0 or num_already_present != 0:
            status_str += ' (omitting'

            if num_dups != 0:
                status_str += ' {num_dups} duplicate entries'
            if num_already_present != 0:
                if num_dups != 0:
                    status_str += ' and'
                status_str += f' {num_already_present} already present entries'

            status_str += ')'

        print(status_str)

        if num_added == 0:
            return

        choice = get_user_choice('Proceed?')
        if choice == 'yes':
            self._df = new_df
            print(f'{self.get_name()} now has {len(self._df)} entries')

        return

    def remove(self, other) -> None:
        self._ensure_df()

        if isinstance(other, pd.Index):
            other_idx = other
        else:
            if isinstance(other, Container):
                other_df = other.get_df()
            elif isinstance(other, pd.DataFrame):
                other_df = other
            else:
                raise ValueError(f'Invalid argument type: {type(other)}')

            if len(other_df) == 0:
                return

            other_df = self._reconcile_ids(other_df)

            other_idx = other_df.index

        other_unique = other_idx.unique()

        num_dups = len(other_idx) - len(other_unique)

        new_idx = self._df.index.difference(other_unique, sort=False)

        num_removed = len(self._df) - len(new_idx)
        num_absent = len(other_unique) - num_removed

        status_str = f'{self.get_name()}: removing {other.get_name()}:'
        if num_removed == 0:
            status_str += ' nothing to remove'
        else:
            status_str += f' removing {num_removed} entries'

        if num_dups != 0 or num_absent != 0:
            status_str += ' (omitting'

            if num_dups != 0:
                status_str += ' {num_dups} duplicate entries'
            if num_absent != 0:
                if num_dups != 0:
                    status_str += ' and'
                status_str += f' {num_absent} absent entries'

            status_str += ')'

        print(status_str)

        if num_removed == 0:
            return

        choice = get_user_choice('Proceed?')
        if choice == 'yes':
            self._df = self._df.loc[new_idx]

        return

class Doc(Container):
    def __init__(self, name: str):
        super(Doc, self).__init__(f"doc {name}")
        self._doc = djlib_config.docs[name]
        return

    def _read(self):
        return self._doc.read()

    def _write_back(self, df):
        self._doc.write(df)
        return

class SpotifyPlaylist(Container):
    def __init__(self, name: str):
        super(SpotifyPlaylist, self).__init__(f"Spotify playlist {name}")
        self._playlist_name = name
        return

    def _read(self):
        return djlib_config.spotify.get_playlist_tracks(self._playlist_name)

    def _write_back(self, df):
        djlib_config.spotify.replace_playlist_tracks(self._playlist_name, df)
        return

class SpotifyLiked(Container):
    def __init__(self):
        super(SpotifyLiked, self).__init__('Spotify Liked Tracks')
        return

    def _read(self):
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

class RekordboxPlaylist(Container):
    def __init__(self, name: str):
        super(RekordboxPlaylist, self).__init__(f"Rekordbox playlist {name}")
        self._playlist_name = name
        return

    def _read(self):
        return djlib_config.rekordbox.get_playlist_tracks(self._playlist_name)

    def _write_back(self, df):
        djlib_config.rekordbox.create_playlist(self._playlist_name, df, overwrite=True)
        return







