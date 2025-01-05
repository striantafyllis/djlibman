
import sys

import pandas as pd

from djlibman import *


def convert_listening_history():
    listening_history = Doc('listening_history')

    old_df = listening_history.get_df()

    new_df = pd.DataFrame(
        index=pd.Index(name='spotify_id', data=old_df.index)
    )

    new_df['spotify_id'] = old_df.id
    new_df['name'] = old_df.name
    new_df['artist_ids'] = old_df.artists.apply(lambda artist_list: '|'.join([artist['id'] for artist in artist_list]))
    new_df['artist_names'] = old_df.artists.apply(lambda artist_list: '|'.join([artist['name'] for artist in artist_list]))
    new_df['duration_ms'] = old_df.duration_ms
    new_df['release_date'] = old_df.album.apply(lambda album: album['release_date'])
    new_df['popularity'] = old_df.popularity
    new_df['added_at'] = old_df.added_at
    new_df['album_id'] = old_df.album.apply(lambda album: album['id'])
    new_df['album_name'] = old_df.album.apply(lambda album: album['name'])

    new_df = new_df.convert_dtypes()

    listening_history_new = Doc('listening_history_new', create=True)
    listening_history_new.append(new_df, prompt=False)
    listening_history_new.write()

    return


def convert_rekordbox_to_spotify():
    rekordbox_to_spotify = Doc('rekordbox_to_spotify')

    old_df = rekordbox_to_spotify.get_df()

    new_df = pd.DataFrame(
        index=pd.Index(name='rekordbox_id', data=old_df.index)
    )

    def artist_converter(track):
        artist_list = track['artists']

        assert isinstance(artist_list, list)

        return '|'.join([artist['id'] for artist in artist_list])

    new_df['rekordbox_id'] = old_df.rekordbox_id
    new_df['spotify_id'] = old_df.spotify_id
    new_df['name'] = old_df.name
    # new_df['artist_ids'] = old_df.apply(artist_converter, axis=1)
    new_df['artist_ids'] = old_df.artists.apply(
        lambda artist_list:
            pd.NA if not isinstance(artist_list, list) and pd.isna(artist_list) else
            '|'.join([artist['id'] for artist in artist_list])
        )
    new_df['artist_names'] = old_df.artists.apply(
        lambda artist_list:
            pd.NA if not isinstance(artist_list, list) and pd.isna(artist_list) else
            '|'.join([artist['name'] for artist in artist_list])
    )
    new_df['duration_ms'] = old_df.duration_ms
    new_df['release_date'] = old_df.album.apply(
        lambda album: album['release_date']
        if not pd.isna(album)
        else pd.NA
    )
    new_df['popularity'] = old_df.popularity
    new_df['added_at'] = new_df['release_date']
    new_df['album_id'] = old_df.album.apply(
        lambda album: album['id']
        if not pd.isna(album)
        else pd.NA
    )
    new_df['album_name'] = old_df.album.apply(
        lambda album: album['name']
        if not pd.isna(album)
        else pd.NA
    )

    new_df = new_df.convert_dtypes()

    rekordbox_to_spotify_new = Doc('rekordbox_to_spotify_new', create=True)
    rekordbox_to_spotify_new.append(new_df, prompt=False)
    rekordbox_to_spotify_new.write()

    return


def remove_artist_old_entries_from_listening_history(
        artist_name,
        cutoff_date
):
    artist_id = find_spotify_artist(artist_name)

    cutoff_date = pd.to_datetime(cutoff_date, utc=True)

    listening_history = Doc('listening_history')

    def condition(track):
        has_artist = False
        for artist in track.artists:
            if artist['id'] == artist_id:
                has_artist = True

        if not has_artist:
            return False

        if track.added_at > cutoff_date:
            return False

        return True

    bool_array = listening_history.get_df().apply(condition, axis=1)

    removed_tracks = listening_history.get_df().loc[bool_array]
    remaining_tracks = listening_history.get_df().loc[~bool_array]

    # continue here

    return


def read_file(filename):
    fh = open(filename)

    lines = []

    for line in fh:
        line = line.strip()
        if line.startswith('#'):
            continue

        lines.append(line)

    fh.close()

    return lines


def go_through_artist_list():
    artists = read_file('./sample_artists.txt')

    for artist in artists:
        sample_artist_to_queue(artist)

    return

def main():
    artist_albums = spotify_discography.get_artist_albums('Dany Dz')

    artist_album = artist_albums.iloc[0]

    album_tracks = spotify_discography.get_album_tracks(artist_album['album_id'], artist_album['name'])

    return

if __name__ == '__main__':
    main()
    sys.exit(0)

