

from internal_utils import *
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
