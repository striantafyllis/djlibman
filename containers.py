
"""
Wraps our various dataframe-like data objects - docs. Spotify playlists, Rekordbox playlists etc.
- into objects that you can manipulate and write back. These data objects can be added to each other,
subtracted from each other etc.
"""
from typing import Union

import numpy as np
import pandas as pd

from general_utils import *

import djlib_config


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


class Container(object):
    """Root of the hierarchy; abstract class"""
    def __init__(self, name: str, *, create=False, modify=True, overwrite=False, prompt=None):
        self._name = name
        self._df = None
        self._changed = False
        self._exists = self._check_existence()
        self._modify = modify
        self._overwrite = overwrite
        self._prompt = prompt

        if not self._exists and not create:
            raise RuntimeError(f'{name} does not exist')

        return

    def _should_prompt(self, local_prompt):
        if local_prompt is not None:
            return local_prompt
        if self._prompt is not None:
            return self._prompt

        # if write-back is not allowed, don't prompt, as modifications aren't permanent
        return self._modify


    def _get_index_name(self):
        raise NotImplementedError()

    def _check_existence(self):
        raise NotImplementedError()

    def _read(self, force=False) -> pd.DataFrame:
        raise NotImplementedError()

    def _write_back(self, df: pd.DataFrame) -> None:
        raise NotImplementedError()

    def _ensure_df(self, force=False):
        if force or self._df is None:
            if self._exists:
                self._df = self._read(force=force)
            else:
                self._df = pd.DataFrame()
        return

    def _rvalue_check(self, operation):
        """Placeholder for checking whether this container can be added to / removed from
        another container"""
        return

    def get_name(self) -> str:
        return self._name

    def exists(self):
        return self._exists

    def get_df(self, force=False) -> pd.DataFrame:
        self._ensure_df(force=force)
        return self._df

    def slice(self,
              from_index=None,
              to_index=None,
              index_column=None,
              ignore_case=False,
              use_prefix=False,
              unambiguous_prefix=False):
        """
        Returns a slice of the underlying dataframe. CAUTION: both from_index and to_index are inclusive.
        from_index and to_index can be specified either as integers or as strings. If they are
        specified as strings, the strings will be looked up in the index_column.
        If ignore_case is set to true and from_index or to_index is a string, the search in the
        index column will be case-insensitive.
        If use_prefix is set to true and from_index or to_index is a string, we'll search the
        index_column for an entry that starts with from_index or to_index respectively. If there
        are more than one such entries, then:
        - if unambiguous_prefix is true, an error will be issued.
        - otherwise, the first such entry will be used.

        If either from_index or to_index are strings and index_column is not specified, an error is issued.
        """
        self._ensure_df()

        if from_index is None:
            from_index = 0
        if to_index is None:
            to_index = len(self._df) - 1

        if isinstance(from_index, int):
            pass
        elif isinstance(from_index, str):
            if index_column is None:
                raise ValueError('from_index is a string but no index_column specified')
            col = self._df[index_column]

            from_index = first_index_of(col, from_index,
                                        ignore_case=ignore_case,
                                        use_prefix=use_prefix,
                                        unambiguous_prefix=unambiguous_prefix)

            if from_index == -1:
                raise ValueError(f'from_index {from_index} not found')

        if isinstance(to_index, int):
            pass
        elif isinstance(to_index, str):
            if index_column is None:
                raise ValueError('from_index is a string but no index_column specified')
            col = self._df[index_column]

            to_index = first_index_of(col, to_index,
                                      ignore_case=ignore_case,
                                      use_prefix=use_prefix,
                                      unambiguous_prefix=unambiguous_prefix)

            if to_index == -1:
                raise ValueError(f'to_index {to_index} not found')

        return self._df.iloc[from_index:to_index+1]

    def __len__(self):
        return len(self.get_df())

    def set_df(self, df: pd.DataFrame) -> None:
        if self._exists and len(self) > 0 and not self._overwrite:
            raise RuntimeError(f'{self._name} cannot be replaced')
        self._df = self._reconcile_ids(df)
        self._changed = True
        return

    def write(self, force=False, **kwargs) -> None:
        if self._df is None:
            raise Exception('Nothing to write')

        if not self._modify:
            raise RuntimeError(f'{self._name} cannot be written back')

        if not self._changed and not force:
            return

        self._write_back(self._df, **kwargs)
        self._changed = False
        return

    def _reconcile_ids(self, other_df):
        self._ensure_df()

        if len(other_df) == 0:
            return other_df

        this_index_name = self._df.index.name if self._df is not None else None
        if this_index_name is None and (self._df is None or len(self._df) == 0):
            this_index_name = self._get_index_name()
            if this_index_name is None:
                return other_df

        if this_index_name == other_df.index.name:
            return other_df

        if this_index_name in other_df.columns:
            other_df = other_df.set_index(this_index_name, drop=False)
            return other_df

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

    def deduplicate(self, prompt=None) -> None:
        self._ensure_df()

        this_df = self._df

        dup_bool_array = this_df.index.duplicated()
        num_dups = dup_bool_array.sum()

        if num_dups > 0:
            print(f'{self.get_name()}: removing {num_dups} duplicate entries')
            if self._should_prompt(prompt):
                choice = get_user_choice('Remove?')
                if choice != 'yes':
                    return

            this_df_uniq = this_df.loc[~dup_bool_array]

            assert len(this_df_uniq) == len(this_df) - num_dups

            self._df = this_df_uniq

            self._changed = True

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

        return self._df.loc[new_idx]

    def get_intersection(self, other):
        return self._get_set_operation_result(other, 'intersection')

    def get_difference(self, other):
        return self._get_set_operation_result(other, 'difference')

    def get_filtered(self, filter):
        self._ensure_df()

        bool_array = self._df.apply(filter, axis=1)
        return pd.DataFrame(self._df.loc[bool_array])

    def _preprocess_before_append(self, df: pd.DataFrame):
        return df

    def append(self, other, prompt=None, silent=False) -> None:
        self._ensure_df()

        if isinstance(other, Container):
            other._rvalue_check('append')
            other_df = other.get_df()
            other_name = other.get_name()
        elif isinstance(other, pd.DataFrame):
            other_df = other
            other_name = None
        else:
            raise ValueError(f'Invalid argument type: {type(other)}')

        if len(other_df) == 0:
            return

        other_df = self._reconcile_ids(other_df)

        other_unique = other_df[~other_df.index.duplicated()]

        num_dups = len(other_df) - len(other_unique)

        assert num_dups >= 0

        if len(self._df) == 0:
            other_unique = self._preprocess_before_append(other_unique)

            new_df = other_unique
            num_added = len(other_unique)
            num_already_present = 0
        else:
            # remove entries that are already there

            this_idx = self._df.index

            genuinely_new_entries_idx = other_unique.index.difference(this_idx, sort=False)

            genuinely_new_entries = other_unique.loc[genuinely_new_entries_idx]

            num_already_present = len(other_unique) - len(genuinely_new_entries)
            num_added = len(genuinely_new_entries)

            genuinely_new_entries = self._preprocess_before_append(genuinely_new_entries)

            if len(genuinely_new_entries) > 0:
                new_df = pd.concat([self._df, genuinely_new_entries])

        assert num_already_present >= 0
        assert num_added >= 0

        if not silent:
            status_str = f'{self.get_name()}:'
            if other_name is not None:
                status_str += f' adding {other_name}:'
            if num_added == 0:
                status_str += ' nothing to add'
            else:
                status_str += f' adding {num_added} entries'

            if num_dups != 0 or num_already_present != 0:
                status_str += ' (omitting'

                if num_dups != 0:
                    status_str += f' {num_dups} duplicate entries'
                if num_already_present != 0:
                    if num_dups != 0:
                        status_str += ' and'
                    status_str += f' {num_already_present} already present entries'

                status_str += ')'

            print(status_str)

        if num_added == 0:
            return

        if self._should_prompt(prompt):
            choice = get_user_choice('Proceed?')
            if choice != 'yes':
                return

        self._df = new_df
        if not silent:
            print(f'{self.get_name()} now has {len(self._df)} entries')
        self._changed = True

        return

    def remove(self, other, prompt=None, silent=False) -> None:
        self._ensure_df()

        if len(self._df) == 0:
            return

        if isinstance(other, pd.Index):
            other_idx = other
        else:
            if isinstance(other, Container):
                other._rvalue_check('remove')
                other_df = other.get_df()
                other_name = other.get_name()
            elif isinstance(other, pd.DataFrame):
                other_df = other
                other_name = None
            else:
                raise ValueError(f'Invalid argument type: {type(other)}')

            if len(other_df) == 0:
                return

            other_df = self._reconcile_ids(other_df)

            other_idx = other_df.index

        this_df = self._df

        other_unique = other_idx.unique()

        num_dups = len(other_idx) - len(other_unique)

        new_idx = this_df.index.difference(other_unique, sort=False)

        num_removed = len(this_df) - len(new_idx)
        num_absent = len(other_unique) - len(this_df.index.unique()) + len(new_idx.unique())

        assert num_dups >= 0
        assert num_removed >= 0
        assert num_absent >= 0

        if not silent:
            status_str = f'{self.get_name()}:'
            if other_name is not None:
                status_str += f' removing {other.get_name()}:'
            if num_removed == 0:
                status_str += ' nothing to remove'
            else:
                status_str += f' removing {num_removed} entries'

            if num_dups != 0 or num_absent != 0:
                status_str += ' (omitting'

                if num_dups != 0:
                    status_str += f' {num_dups} duplicate entries'
                if num_absent != 0:
                    if num_dups != 0:
                        status_str += ' and'
                    status_str += f' {num_absent} absent entries'

                status_str += ')'

            print(status_str)

        if num_removed == 0:
            return

        if self._should_prompt(prompt):
            choice = get_user_choice('Proceed?')
            if choice != 'yes':
                return
        this_df = this_df.loc[new_idx]

        self._df = this_df

        self._changed = True

        return

    def truncate(self, prompt=None, silent=False):
        if len(self) == 0:
            return

        if not silent:
            print(f'{self.get_name()}: truncating, removing {len(self)} entries')

        if self._should_prompt(prompt):
            choice = get_user_choice('Proceed?')
            if choice != 'yes':
                return

        self._df = self._df[0:0]
        self._changed = True

        return

    def sort(self, column, ascending=True):
        self._ensure_df()

        self._df = self._df.sort_values(by=column, ascending=ascending, axis=0)

        return


class Doc(Container):
    def __init__(self,
                 name: str, *,
                 modify=True,
                 create=False,
                 overwrite=False,
                 index_name=None,
                 **kwargs):
        if name in djlib_config.docs:
            self._doc = djlib_config.docs[name]
            if len(kwargs) > 0:
                raise ValueError(f'Doc {name} is in config but doc args {kwargs.keys()} were also specified')
        else:
            self._doc = djlib_config.create_doc(name, **kwargs)

        super(Doc, self).__init__(f"doc {name}",
                                  modify=modify,
                                  create=create,
                                  overwrite=overwrite)

        self._index_name = index_name
        return

    def _check_existence(self):
        return self._doc.exists()

    def getmtime(self):
        return self._doc.getmtime()

    def delete(self):
        if self.exists():
            self._doc.delete()
        self._exists = False

    def _read(self, force=False):
        return self._doc.read(force=force)

    def _write_back(self, df):
        self._doc.write(df)
        return

    def _get_index_name(self):
        if self._index_name is None:
            if self._doc._index_column is not None and self._doc._index_column != '_FIRST_COLUMN':
                return self._doc._index_column
            self._ensure_df()
            if self._df is not None:
                return self._df.index.name
        return self._index_name


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

class Wrapper(Container):
    def __init__(self, contents: Union[Container, pd.DataFrame], name=None, index_name=None):
        if isinstance(contents, Container):
            if name is None:
                name = 'Wrapper(' + contents.get_name() + ')'
            if index_name is None:
                index_name = contents._get_index_name()
        elif isinstance(contents, pd.DataFrame):
            pass
        else:
            raise ValueError(f'Invalid wrapper contents type: {type(contents)}')

        if name is None:
            name = 'Wrapper'

        super(Wrapper, self).__init__(
            name=name,
            modify=False,
            create=False,
            overwrite=False
        )

        self._contents = contents
        self._index_name = index_name
        return

    def _check_existence(self):
        return True

    def _read(self, force=False):
        if isinstance(self._contents, pd.DataFrame):
            return self._contents
        elif isinstance(self._contents, Container):
            return self._contents.get_df()
        else:
            assert False

    def _write_back(self, df):
        raise NotImplementedError()

    def _get_index_name(self):
        if self._index_name is None:
            raise RuntimeError(f'{self.get_name()} is empty and a default index name has not been provided')
        return self._index_name


