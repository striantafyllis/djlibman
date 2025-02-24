
import sys

import pandas as pd

from djlibman import *
import spotify_discography_v1
import spotify_discography
from classification import *


def convert_listening_history():
    listening_history = ListeningHistory()

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
    artist_id = spotify_discography.find_spotify_artist(artist_name)

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

def read_file_with_numbers(filename):
    fh = open(filename)

    lines = []

    for line in fh:
        comment_start = line.find('#')
        if comment_start != -1:
            line = line[:comment_start]

        line = line.strip()
        if line == '':
            continue

        pieces = line.split()

        num = None
        string = None
        if len(pieces) >= 2:
            try:
                num = int(pieces[-1])
                string = ' '.join(pieces[:-1])
            except ValueError:
                pass

        if string is None:
            string = ' '.join(pieces)

        lines.append((string, num))

    fh.close()

    return lines

def go_through_artist_list():
    artists = read_file_with_numbers('data/ready_artists.txt')

    for artist, number in artists:
        sample_artist_to_queue(artist, latest=number, popular=number)

    return


def library_reorg_add_question_mark():
    djlib = Doc('djlib')
    bool_array = djlib.get_df().apply(
        lambda track: classification.track_is(track, classes=['A', 'B'], before='2024-10-19'),
        axis=1
    )

    old_tracks_idx = djlib.get_df().index[bool_array]

    progressive = RekordboxPlaylist(name=['managed', 'Progressive'])
    progressive_idx = progressive.get_df().index

    old_tracks_idx = old_tracks_idx.difference(progressive_idx, sort=False)

    mixes = [
        'mix 19b - asiento - prog',
        'mix 18 - asiento - latin jazzy',
        'mix 19 - asiento - prog',
        'mix 17 - george - prog',
        'mix 14 afro',
        'mix 13 summer',
        'mix 12 middle east'
    ]

    for mix in mixes:
        mix_playlist = RekordboxPlaylist(name=['Mixes', mix])
        mix_idx = mix_playlist.get_df().index

        old_tracks_idx = old_tracks_idx.difference(mix_idx, sort=False)

    djlib.get_df().loc[old_tracks_idx, 'Class'] = '?' + djlib.get_df().loc[old_tracks_idx, 'Class']

    djlib.write(force=True)

    return

def promote_set_tracks_to_a():
    rb_playlists = djlib_config.rekordbox.get_playlist_names()

    sets = [['Sets', name] for name in rb_playlists['Sets']]

    for set in sets:
        promote_tracks_to_a(set)

    return

def get_progressive_a_producers():
    djlib = Doc('djlib')
    listening_history = Doc('listening_history')

    prog_a_tracks = add_spotify_fields_to_rekordbox(
        filter_tracks(
            djlib.get_df(),
            classes=['A'],
            flavors=['Progressive', 'Progressive-Adjacent']
    ), drop_missing_ids=True)

    prog_a_artists = get_track_artists(prog_a_tracks)

    a_tracks = add_spotify_fields_to_rekordbox(
        filter_tracks(
            djlib.get_df(),
            classes=['A']
    ), drop_missing_ids=True)

    b_tracks = add_spotify_fields_to_rekordbox(
        filter_tracks(
            djlib.get_df(),
            classes=['B']
    ), drop_missing_ids=True)
    other_tracks = add_spotify_fields_to_rekordbox(
        filter_tracks(
            djlib.get_df(),
            not_classes=['A', 'B']
    ), drop_missing_ids=True)
    listened_tracks = listening_history.get_df()

    add_artist_track_counts(prog_a_artists, a_tracks, track_count_column='A')
    add_artist_track_counts(prog_a_artists, b_tracks, track_count_column='B')
    add_artist_track_counts(prog_a_artists, other_tracks, track_count_column='CX?')
    add_artist_track_counts(prog_a_artists, listened_tracks, track_count_column='Listened')

    prog_a_artists.sort_values(by='A', ascending=False, inplace=True)

    doc = Doc('prog_a_artists', create=True, overwrite=True,
              path='/Users/spyros/python/djlibman/data/prog_a_artists.csv',
              backups=0,
              type='csv'
              )
    doc.set_df(prog_a_artists)
    doc.write()

    return

def discog_report_for_prog_a_producers(cache_only=False):
    source_doc = Doc(
        'prog_a_artists',
        path='/Users/spyros/python/djlibman/data/prog_a_artists.csv',
        backups=0,
        type='csv'
        )

    prog_a_artists = source_doc.get_df()

    i = 0
    for artist in prog_a_artists.itertuples(index=False):
        i += 1

        # if i > 4:
        #     break

        artist_id = artist.artist_id
        artist_name = artist.artist_name

        print(f'** Getting discography for artist {artist_id} {artist_name}... ', end='')

        start_time = time.time()
        result = spotify_discography_v1.get_artist_discography(artist_name, artist_id=artist_id, cache_only=cache_only)
        end_time = time.time()

        if result is None:
            print(f' (no result)', end='')
        else:
            print(f' {len(result)} tracks', end='')

        print(f' {end_time - start_time:.1f} seconds')

    return



def old_main():
    # get_progressive_a_producers()

    # promote_tracks_in_spotify_queue(last_track='Storming', promote_source_name='L1 queue', promote_target_name='L2 queue')

    # text_file_to_spotify_playlist('data/tracks.txt', target_playlist_name='tmp queue')

    return

def main():
    # discogs = spotify_discography.get_instance()

    # old script: 115.9 seconds

    start_time = time.time()
    discog_report_for_prog_a_producers(cache_only=False)
    end_time = time.time()

    print(f'Total time: {end_time - start_time:.1f} seconds')

    return


if __name__ == '__main__':
    main()
    sys.exit(0)
