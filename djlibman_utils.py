"""
Utility functions that depend on the djlibman infrastructure
"""

from general_utils import *
from djlib_config import *


def add_spotify_ids(rekordbox_tracks, include_missing_ids=False):
    rekordbox_to_spotify_mapping = docs['rekordbox_to_spotify'].read()

    if not include_missing_ids:
        # remove empty mappings
        rekordbox_to_spotify_mapping = rekordbox_to_spotify_mapping.loc[
            ~pd.isna(rekordbox_to_spotify_mapping.spotify_id)
        ]

    rekordbox_tracks_with_spotify_id = rekordbox_tracks.merge(
        right=rekordbox_to_spotify_mapping,
        how='inner',
        left_index=True,
        right_index=True
    )[rekordbox_tracks.columns.to_list() + ['spotify_id']]

    return rekordbox_tracks_with_spotify_id

def translate_to_spotify_ids(rekordbox_ids, include_missing_ids=False):
    rekordbox_to_spotify_mapping = docs['rekordbox_to_spotify'].read()

    if not include_missing_ids:
        # remove empty mappings
        rekordbox_to_spotify_mapping = rekordbox_to_spotify_mapping.loc[
            ~pd.isna(rekordbox_to_spotify_mapping.spotify_id)
        ]

    spotify_ids_df = rekordbox_to_spotify_mapping.loc[rekordbox_ids]

    return spotify_ids_df


def add_to_doc(doc_name: str, new_entries_name: str, new_entries: pd.DataFrame):
    if len(new_entries) == 0:
        return

    doc = docs[doc_name]

    doc_entries = doc.read()

    new_entries_len_with_dups = len(new_entries)
    new_entries = dataframe_ensure_unique_index(new_entries)

    num_dups = new_entries_len_with_dups - len(new_entries)

    if len(doc_entries) == 0:
        new_doc_entries = new_entries
    else:
        if doc_entries.index.name != new_entries.index.name:
            raise ValueError(f'{doc_name} has index {doc_entries.index.name} but new entries have '
                             f'index {new_entries.index.name}')

        for column in doc_entries.columns:
            if column not in new_entries.columns:
                new_entries[column] = np.nan

        # this takes care of extra columns, columns in different order etc.
        new_entries = new_entries[doc_entries.columns]

        # remove entries that are already there
        genuinely_new_entries_idx = new_entries.index.difference(doc_entries.index, sort=False)

        genuinely_new_entries = new_entries.loc[genuinely_new_entries_idx]

        num_already_present = len(new_entries) - len(genuinely_new_entries)
        num_added = len(genuinely_new_entries)

        new_doc_entries = pd.concat([doc_entries, genuinely_new_entries])

    status_str = f'{doc_name}: adding {new_entries_name}:'
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
        doc.write(new_doc_entries)
        print(f'{doc_name} now has {len(new_doc_entries)} entries')

    return

def remove_from_doc(doc_name: str, remove_entries_name: str, remove_entries):
    if len(remove_entries) == 0:
        return

    doc = docs[doc_name]
    doc_entries = doc.read()

    if isinstance(remove_entries, pd.DataFrame):
        remove_idx = remove_entries.index
    elif isinstance(remove_entries, pd.Index):
        remove_idx = remove_entries
    else:
        remove_idx = pd.Index(remove_entries)

    remove_idx = remove_idx.unique()

    num_dups = len(remove_entries) - len(remove_idx)

    new_doc_idx = doc_entries.index.difference(remove_idx, sort=False)

    num_removed = len(doc_entries) - len(new_doc_idx)
    num_absent = len(remove_idx) - len(doc_entries) + len(new_doc_idx)

    status_str = f'{doc_name}: removing {remove_entries_name}:'
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
        doc_entries = doc_entries.loc[new_doc_idx]
        doc.write(doc_entries)

    return
