"""
Initializes the new discography (v2) from discography v1 files
"""

import sys
import os
import os.path
import re
from fileinput import filename

from djlibman import *

def create_spotify_albums():
    all_cache_files = os.listdir(djlib_config.discography_cache_dir)

    artist_albums_files = [
        filename
        for filename in all_cache_files
        if re.match(r'^artist-albums-.*\.csv$', filename)
    ]

    print(f'Found {len(artist_albums_files)} artist albums files in cache')
    print()

    spotify_albums = Doc('spotify_albums', create=True, overwrite=True)

    total_albums = 0
    for artist_albums_fn in artist_albums_files:
        print(f'Reading {artist_albums_fn}... ', end='')

        artist_albums_doc = Doc(artist_albums_fn, modify=False,
                                path=os.path.join(djlib_config.discography_cache_dir, artist_albums_fn),
                                type='csv',
                                datetime_columns=['release_date'])

        print(f'{len(artist_albums_doc)} albums')
        total_albums += len(artist_albums_doc)

        if len(spotify_albums) == 0:
            spotify_albums.set_df(artist_albums_doc.get_df())
        else:
            spotify_albums.append(artist_albums_doc, prompt=False)

    print(f'{total_albums} albums read; {len(spotify_albums)} albums left after deduplication')
    spotify_albums.write()

    return

def create_spotify_artist_albums_last_check():
    all_cache_files = os.listdir(djlib_config.discography_cache_dir)

    artist_albums_files = [
        filename
        for filename in all_cache_files
        if re.match(r'^artist-albums-.*\.csv$', filename)
    ]

    print(f'Found {len(artist_albums_files)} artist albums files in cache')
    print()

    df = pd.DataFrame(data=[], columns=['artist_id', 'artist_name', 'check_time'],
                      index=pd.Index(data=[], name='artist_id'))
    for artist_albums_fn in artist_albums_files:
        m = re.match(r'^artist-albums-([^\-]+)-(.*).csv$', artist_albums_fn)

        assert m is not None

        artist_id = m.group(1)
        artist_name = m.group(2)
        check_unixtime = os.path.getmtime(
                os.path.join(djlib_config.discography_cache_dir, artist_albums_fn))

        check_time = pd.to_datetime(check_unixtime, unit='s', utc=True)

        df.loc[artist_id] = {
            'artist_id': artist_id,
            'artist_name': artist_name,
            'check_time': check_time
            }

    spotify_artist_albums_last_check = Doc('spotify_artist_albums_last_check',
                                           create=True, overwrite=True)
    spotify_artist_albums_last_check.set_df(df)
    spotify_artist_albums_last_check.write()

    return

def create_spotify_tracks():
    all_cache_files = os.listdir(djlib_config.discography_cache_dir)

    album_tracks_files = [
        filename
        for filename in all_cache_files
        if re.match(r'^album-tracks-.*\.csv$', filename)
    ]

    print(f'Found {len(album_tracks_files)} album tracks files in cache')
    print()

    spotify_tracks = Doc('spotify_tracks', create=True, overwrite=True)

    total_tracks = 0
    for album_tracks_fn in album_tracks_files:
        print(f'Reading {album_tracks_fn}... ', end='')

        album_tracks_doc = Doc(album_tracks_fn, modify=False,
                                path=os.path.join(djlib_config.discography_cache_dir,
                                                  album_tracks_fn),
                                type='csv',
                                index_column='spotify_id',
                                datetime_columns=['release_date', 'added_at'])

        print(f'{len(album_tracks_doc)} tracks')
        total_tracks += len(album_tracks_doc)

        if len(spotify_tracks) == 0:
            spotify_tracks.set_df(album_tracks_doc.get_df())
        else:
            spotify_tracks.append(album_tracks_doc, prompt=False)

    print(f'{total_tracks} tracks read; {len(spotify_tracks)} inserted')
    spotify_tracks.write()

    return


_open_artist_tracks_docs = {}

def _get_artist_tracks_doc(artist_id: str, artist_name: str):
    if artist_id in _open_artist_tracks_docs:
        return _open_artist_tracks_docs[artist_id]

    artist_tracks_dir = os.path.join(djlib_config.default_dir, 'spotify-artist-tracks')

    artist_tracks_files = os.listdir(artist_tracks_dir)

    artist_tracks_file = None
    artist_tracks_pattern = re.compile(r'^artist-tracks-%s-.*\.csv$' % artist_id)
    for file in artist_tracks_files:
        if re.match(artist_tracks_pattern, file):
            artist_tracks_file = file
            break

    if artist_tracks_file is None:
        artist_tracks_file = f'artist-tracks-{artist_id}-{artist_name.replace('/', '-')}.csv'
        new_file = True
    else:
        new_file = False

    artist_tracks_doc = Doc(
        name=f'artist tracks {artist_id} {artist_name}',
        path=os.path.join(artist_tracks_dir, artist_tracks_file),
        type='csv',
        index_column='spotify_id',
        datetime_columns=['release_date', 'added_at'],
        backups=0,
        create=new_file,
        overwrite=new_file,
        modify=True
    )

    _open_artist_tracks_docs[artist_id] = artist_tracks_doc

    return artist_tracks_doc

def _album_to_artist_tracks(album_doc: Doc):
    tracks_by_artist = {}
    artist_names_by_id = {}

    album_df = album_doc.get_df()
    for i in range(len(album_df)):
        artist_ids = album_df.iloc[i]['artist_ids'].split('|')
        artist_names = album_df.iloc[i]['artist_names'].split('|')

        for j, artist_id in enumerate(artist_ids):
            artist_names_by_id[artist_id] = artist_names[j]

            if artist_id not in tracks_by_artist:
                tracks_by_artist[artist_id] = []

            tracks_by_artist[artist_id].append(i)

    print(f'Album file {album_doc.get_name()}: {len(album_doc)} tracks, {len(tracks_by_artist)} artists')

    for artist_id in tracks_by_artist:
        artist_name = artist_names_by_id[artist_id]
        artist_track_indices = tracks_by_artist[artist_id]
        artist_tracks = album_df.iloc[artist_track_indices]

        print(f'    Artist {artist_id} {artist_name}: {len(artist_tracks)} tracks')

        artist_doc = _get_artist_tracks_doc(artist_id, artist_name)

        if len(artist_doc) == 0:
            artist_doc.set_df(artist_tracks)
        else:
            artist_doc.append(artist_tracks, prompt=False)
        artist_doc.write()

    return

def create_artist_tracks():
    all_cache_files = os.listdir(djlib_config.discography_cache_dir)

    album_tracks_files = [
        filename
        for filename in all_cache_files
        if re.match(r'^album-tracks-.*\.csv$', filename)
    ]

    print(f'Found {len(album_tracks_files)} album tracks files in cache')
    print()

    for album_tracks_fn in album_tracks_files:
        print(f'Reading {album_tracks_fn}... ')

        album_tracks_doc = Doc(album_tracks_fn, modify=False,
                                path=os.path.join(djlib_config.discography_cache_dir,
                                                  album_tracks_fn),
                                type='csv',
                                index_column='spotify_id',
                                datetime_columns=['release_date', 'added_at'])

        _album_to_artist_tracks(album_tracks_doc)

    return




def main():
    # create_spotify_albums()
    # create_spotify_artist_albums_last_check()
    # create_spotify_tracks()

    create_artist_tracks()
    return

if __name__ == '__main__':
    main()
