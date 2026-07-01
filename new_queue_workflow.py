
import time

import djlib_config
import spotify_discography

import spyroslib.containers as ct

from containers import ListeningHistory, Queue
from library_workflow import add_spotify_fields_to_rekordbox
from spotify_util import get_track_artists, add_artist_track_counts
from classification import filter_tracks

from local_util import *

def get_A_producers(run_name, *flavors):
    djlib = ct.Doc('djlib')
    listening_history = ListeningHistory()

    a_tracks = add_spotify_fields_to_rekordbox(
        filter_tracks(
            djlib.get_df(),
            classes=['A'],
            flavors=flavors,
        ), drop_missing_ids=True)

    a_artists = get_track_artists(a_tracks)

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

    add_artist_track_counts(a_artists, a_tracks, track_count_column='A')
    add_artist_track_counts(a_artists, b_tracks, track_count_column='B')
    add_artist_track_counts(a_artists, other_tracks, track_count_column='CDX')
    add_artist_track_counts(a_artists, listened_tracks, track_count_column='listened')

    a_artists.sort_values(by='A', ascending=False, inplace=True)

    doc = ct.Doc('a_artists', create=True, overwrite=True,
              path=f'data/a_artists-{run_name}.csv',
              backups=0,
              type='csv'
              )
    doc.set_df(a_artists)
    doc.write()

    return

def discog_report_for_A_producers(run_name):
    source_doc = ct.Doc(
        'a_artists',
        path=f'data/a_artists_reduced-{run_name}.csv',
        backups=0,
        type='csv'
        )

    target_doc = ct.Doc(
        'a_artists_enhanced',
        path=f'data/a_artists_enhanced-{run_name}.csv',
        backups=0,
        type='csv',
        create=True,
        overwrite=True
        )

    a_artists = source_doc.get_df()

    a_artists['total_tracks'] = 0

    discogs = spotify_discography.get_instance()

    i = 0
    for artist in a_artists.itertuples(index=False):
        i += 1

        # if i > 2:
        #     break

        artist_id = artist.artist_id
        artist_name = artist.artist_name

        print(f'** Getting discography for artist {artist_id} {artist_name}... ')

        start_time = time.time()
        result = discogs.get_artist_discography(
            artist_id=artist_id,
            artist_name=artist_name,
            deduplicate_tracks=True,
        )

        a_artists.loc[artist_id, 'total_tracks'] = len(result)

        end_time = time.time()

        if result is None:
            print(f' (no result)', end='')
        else:
            print(f' {len(result)} tracks', end='')

        print(f' {end_time - start_time:.1f} seconds')

    a_artists['frac_listened'] = a_artists.listened / a_artists.total_tracks
    a_artists['frac_selected'] = (a_artists.A + a_artists.B) / a_artists.listened

    target_doc.set_df(a_artists)
    target_doc.write()

    return

def refresh_A_producers(run_name):
    source_doc = ct.Doc(
        'a_artists',
        path=f'data/a_artists_reduced-{run_name}.csv',
        backups=0,
        type='csv'
        )

    a_artists = source_doc.get_df()

    discogs = spotify_discography.get_instance()

    i = 0
    for artist in a_artists.itertuples(index=False):
        i += 1

        # if i <= 2:
        #     continue

        # if i > 3:
        #     break

        artist_id = artist.artist_id
        artist_name = artist.artist_name

        print(f'** Refreshing artist {artist_id} {artist_name}... ')

        start_time = time.time()
        discogs.refresh_artist(
            artist_id=artist_id,
            artist_name=artist_name,
            refresh_days=30,
            force=False)

        end_time = time.time()

        print(f'** Refreshed artist {artist_id} {artist_name}: {end_time-start_time:.1f} seconds')

def sample_artist_to_queue(
        queue_name,
        artist_id=None,
        artist_name=None,
        latest=10,
        total=10,
        latest_cutoff_days=365
    ):
    print(f'Sampling artist {artist_name} to queue...')

    discography = spotify_discography.get_instance()

    artist_discography = ct.Wrapper(
        contents=discography.get_artist_discography(artist_id=artist_id,
                                                    artist_name=artist_name,
                                                    deduplicate_tracks=True),
        name=f'discography for {artist_name}')

    print(f'Artist {artist_name}: Found {len(artist_discography)} tracks')

    listening_history = ListeningHistory()
    queue = Queue(queue_name)

    listening_history.filter(artist_discography, prompt=False, silent=True)
    artist_discography.remove(queue, prompt=False, silent=True)

    print(f'Left after removing listening history and queue: {len(artist_discography)} tracks')

    if total == -1:
        total = sys.maxsize
    if latest == -1:
        latest = sys.maxsize

    if len(artist_discography) < total:
        print(f'Artist {artist_name}: adding all {len(artist_discography)} tracks to queue')
        queue.append(artist_discography, prompt=False)
        queue.write()
        return

    if latest > 0:
        latest_cutoff_date = (pd.Timestamp.utcnow() -
                              pd.Timedelta(value=latest_cutoff_days, unit='days'))

        latest_tracks = artist_discography.get_filtered(
            lambda t: t['release_date'] >= latest_cutoff_date
        )

        if len(latest_tracks) > latest:
            latest_tracks.sort_values(by='release_date', ascending=False, axis=0, inplace=True)
            latest_tracks = latest_tracks[:latest]

        artist_discography.remove(latest_tracks, prompt=False)

        print(f'Artist {artist_name}: Adding {len(latest_tracks)} latest tracks to queue')
        # pretty_print_tracks(latest_tracks, indent=' '*4, enum=True, extra_attribs='release_date')

        queue.append(latest_tracks, prompt=False)
        queue.write()

        remaining = total - len(latest_tracks)
    else:
        remaining = total

    if remaining > 0:
        if len(artist_discography) > remaining:
            artist_discography.sort('popularity', ascending=False)

            most_popular_tracks = artist_discography.get_df()[:remaining]
        else:
            most_popular_tracks = artist_discography.get_df()

        print(f'Artist {artist_name}: adding {len(most_popular_tracks)} older tracks to queue')
        print('Popular tracks:')
        pretty_print_tracks(most_popular_tracks, indent=' '*4, enum=True, extra_attribs='popularity')

        queue.append(most_popular_tracks, prompt=False)
        queue.write()

    return


def populate_queue(run_name, queue_name):
    next_q_artists = ct.Doc(
        'next_q_artists',
        path=f'data/next_q_artists-{run_name}.csv',
        backups=0,
        type='csv'
        )

    i=0
    for artist in next_q_artists.get_df().itertuples(index=False):
        i += 1

        # if i > 1:
        #     break

        sample_artist_to_queue(
            queue_name=queue_name,
            artist_id = artist.artist_id,
            artist_name = artist.artist_name,
            latest=artist.num_latest,
            total=artist.num_total)

    return


def _sample_run():
    # DO NOT RUN

    run_name = 'prog-20260630'
    queue_name = 'prog_queue'

    get_A_producers(run_name, 'Progressive')

    # prepare a_artists_reduced-{run_name}.csv

    refresh_A_producers(run_name)

    discog_report_for_A_producers(run_name)

    # prepare next_q_artists-{run_name}

    populate_queue(run_name, queue_name)

    return








