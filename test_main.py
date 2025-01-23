
import sys

import pandas as pd

from djlibman import *
from spotify_discography import get_artist_discography
from classification import *


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


def read_file(filename):
    fh = open(filename)

    lines = []

    for line in fh:
        comment_start = line.find('#')
        if comment_start != -1:
            line = line[:comment_start]

        line = line.strip()
        if line == '':
            continue

        lines.append(line)

    fh.close()

    return lines

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
    artists = read_file_with_numbers('./ready_artists.txt')

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


def promote_new_mix_tracks_to_a():
    djlib = Doc('djlib')

    new_mixes = [
        'mix 17 - george - prog',
        'mix 19 - asiento - prog',
        'mix 20 - flowtoys - prog',
        'mix 21 - winter blues - prog'
    ]

    track_ids = pd.Index([])
    for mix_name in new_mixes:
        rekordbox_playlist = RekordboxPlaylist(name=['Mixes', mix_name])
        mix_track_ids = rekordbox_playlist.get_df().index

        track_ids = track_ids.union(mix_track_ids, sort=False)

    tracks = djlib.get_df().loc[track_ids]

    print('Tracks in mixes:')
    pretty_print_tracks(tracks, enum=True, ids=False)
    print()

    print('Tracks already at A:')
    tracks_already_A = filter_tracks(tracks, classes=['A'])
    pretty_print_tracks(tracks_already_A, enum=True, ids=False)
    print()

    print('Tracks to be promoted to A:')
    tracks_to_promote = tracks.loc[tracks.index.difference(tracks_already_A.index, sort=False)]
    if len(tracks_to_promote) == 0:
        print('NONE')
    else:
        pretty_print_tracks(tracks_to_promote, enum=True, ids=False)
        choice = get_user_choice('Proceed?')
        if choice == 'yes':
            djlib.get_df().loc[tracks_to_promote.index, 'Class'] = 'A'
            djlib.write(force=True)

    return

def form_progressive_not_used():
    prog_mixes = [
        'mix 17 - george - prog',
        'mix 19 - asiento - prog',
        'mix 20 - flowtoys - prog',
        'mix 21 - winter blues - prog'
    ]

    prog_tracks = RekordboxPlaylist(['managed', 'Progressive'])

    prog_not_used_tracks = RekordboxPlaylist(['managed', 'Progressive Not Used'],
                                             create=True, overwrite=True)
    prog_not_used_tracks.append(prog_tracks, prompt=False)

    for mix in prog_mixes:
        mix_tracks = RekordboxPlaylist(['Mixes', mix])
        prog_not_used_tracks.remove(mix_tracks, prompt=False)

    prog_not_used_tracks.write()

    return




def main():
    # go_through_artist_list()

    # playlists_maintenance(do_rekordbox=True, do_spotify=True)

    # library_maintenance()

    # promote_new_mix_tracks_to_a()

    form_progressive_not_used()

    return

if __name__ == '__main__':
    main()
    sys.exit(0)

