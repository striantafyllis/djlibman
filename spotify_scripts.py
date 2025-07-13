
import random

from spotipy import Spotify

import djlib_config
import library_scripts
import spotify_discography
import classification
from containers import *
from library_scripts import add_spotify_fields_to_rekordbox
from spotify_util import *

def sanity_check_disk_queues():
    queue = Queue()
    print(f'Queue: {len(queue)} tracks')

    listening_history = ListeningHistory()
    print(f'Listening history: {len(listening_history)} tracks')

    library = RekordboxPlaylist('Main Library')
    print(f'Library: {len(library)} tracks')

    # entries in the queue should be unique
    queue.deduplicate()

    # entries in listening history should be unique
    listening_history.deduplicate()

    # queue tracks should not be in listening history
    listening_history.filter(queue)

    # library tracks should be in listening history and should not be in queue
    listening_history.append(library)
    queue.remove(library)

    queue.write()
    listening_history.write()

    return

def separate_chosen_tracks(
        tracks,
        method='Liked',
        reference_tracks=None
):
    """
    Separates the "chosen" from the "non-chosen" tracks in a playlist.
    The case-insensitive method argument specifies the way:
    - "liked": Tracks that are liked are chosen; the rest are not chosen.
    - "ref": Tracks in the reference tracks are chosen; the rest are not chosen.
    - "liked+ref": Tracks that are both liked and in the reference tracks are chosen;
            the rest are not chosen.

    Returns a pair of values: (chosen tracks, not chosen tracks)
    """
    method = method.lower()

    if isinstance(tracks, Container):
        tracks = tracks.get_df()

    if reference_tracks is not None and isinstance(reference_tracks, Container):
        # TODO translate IDs
        reference_tracks = reference_tracks.get_df()

    if method == 'liked':
        refs = [SpotifyLiked().get_df()]
    elif method == 'ref':
        if reference_tracks is None:
            raise ValueError('Reference tracks not provided')
        refs = [reference_tracks]
    elif method == 'liked+ref':
        if reference_tracks is None:
            raise ValueError('Reference tracks not provided')
        refs = [SpotifyLiked().get_df(), reference_tracks]
    else:
        raise ValueError(f"Unrecognizable method '{method}'")

    chosen_idx = tracks.index

    for ref in refs:
        chosen_idx = chosen_idx.intersection(ref.index, sort=False)

    not_chosen_idx = tracks.index.difference(chosen_idx, sort=False)

    return tracks.loc[chosen_idx], tracks.loc[not_chosen_idx]




def promote_tracks_in_spotify_queue(
        last_track,
        promote_source_name,
        promote_target_name,
        method,
        ref_playlist
):

    promote_queue_level = djlib_config.get_spotify_queue_level(promote_source_name)

    promote_source = SpotifyPlaylist(promote_source_name)
    promote_target = SpotifyPlaylist(promote_target_name)

    listened_tracks = promote_source.slice(
        from_index=0,
        to_index=last_track,
        index_column='name',
        ignore_case=True,
        use_prefix=True,
        unambiguous_prefix=True
    )

    print(f'{promote_source_name}: {len(listened_tracks)} listened tracks')
    pretty_print_tracks(listened_tracks, indent=' ' * 4, enum=True)
    choice = get_user_choice('Is this correct?')
    if choice != 'yes':
        return

    if len(listened_tracks) == 0:
        return

    # find how many of the listened tracks are liked
    listened_chosen_tracks, listened_not_chosen_tracks = separate_chosen_tracks(
        listened_tracks,
        method=method,
        reference_tracks=SpotifyPlaylist(ref_playlist) if ref_playlist is not None else None
    )

    print(f'{promote_source_name}: {len(listened_chosen_tracks)} of the '
          f'{len(listened_tracks)} listened tracks are chosen')
    pretty_print_tracks(listened_chosen_tracks, indent=' ' * 4, enum=True)
    choice = get_user_choice('Is this correct?')
    if choice != 'yes':
        return
    print()

    if len(listened_chosen_tracks) > 0:
        promote_target.append(listened_chosen_tracks, prompt=False)
        promote_target.write()

        liked = SpotifyLiked()

        liked.remove(listened_chosen_tracks, prompt=False)
        liked.write()

    promote_source.remove(listened_tracks, prompt=False)
    promote_source.write()

    if promote_queue_level == 1:
        print(f'Removing listened tracks from disk queue...')
        queue = Queue()
        queue.remove(listened_tracks, prompt=False)
        queue.write()

    print(f'Adding listened tracks to listening history...')
    listening_history = ListeningHistory()
    listening_history.append(listened_tracks, prompt=False)
    listening_history.write()

    return

def replenish_spotify_queue(
        queue_name='L1 queue',
        target_size=100):
    spotify_queue = SpotifyPlaylist(queue_name)
    disk_queue = Queue()

    tracks_wanted = target_size - len(spotify_queue)
    if tracks_wanted <= 0:
        print(f'Spotify playlist {queue_name} already has {len(spotify_queue)} tracks; no replenishment needed.')
        return

    candidate_tracks_idx = disk_queue.get_df().index.difference(spotify_queue.get_df().index)

    if len(candidate_tracks_idx) == 0:
        print(f'Disk queue has no other tracks; no replenishment possible.')
        return

    num_tracks_to_add = min(tracks_wanted, len(candidate_tracks_idx))

    choice = get_user_choice(f'Add {tracks_wanted} new tracks to {queue_name}?')
    if choice == 'yes':
        if num_tracks_to_add < len(candidate_tracks_idx):
            tracks_to_add_idx = random.sample(candidate_tracks_idx.to_list(), k=num_tracks_to_add)
            tracks_to_add = disk_queue.get_df().loc[tracks_to_add_idx]
        else:
            tracks_to_add = disk_queue.get_df().loc[candidate_tracks_idx]

        print(f'Adding {num_tracks_to_add} tracks to {queue_name}')
        pretty_print_tracks(tracks_to_add, indent=' '*4, enum=True)

        spotify_queue.append(tracks_to_add, prompt=False)
        spotify_queue.write()

    return

def sanity_check_spotify_queue(spotify_queue_name, *, is_level_1=False, is_promote_queue=False):
    spotify_queue = SpotifyPlaylist(spotify_queue_name)
    print(f'{spotify_queue_name}: {len(spotify_queue)} tracks')

    if len(spotify_queue) == 0:
        return

    # Make sure items in the queue are unique
    spotify_queue.deduplicate()

    listening_history = ListeningHistory()

    if is_level_1:
        listening_history.filter(spotify_queue)
        spotify_queue.write()
    else:
        # Make sure all items in the L2+ queues are already in the listening history
        listening_history.append(spotify_queue, prompt=False)
        listening_history.write()

    # Make sure items in the queue are not already in the library
    library = RekordboxPlaylist('Main Library')

    queue_tracks_in_library = spotify_queue.get_intersection(library)
    if len(queue_tracks_in_library) > 0:
        print(f'WARNING: {len(queue_tracks_in_library)} tracks are already in the library')
        choice = get_user_choice('Remove?')
        if choice == 'yes':
            spotify_queue.remove(queue_tracks_in_library, prompt=False)

    # Make sure all items in the queue are not liked
    if not is_promote_queue:
        spotify_liked = SpotifyLiked()

        queue_liked_tracks = spotify_queue.get_intersection(spotify_liked)

        if len(queue_liked_tracks) > 0:
            print(f'WARNING: {len(queue_liked_tracks)} {spotify_queue_name} tracks are already liked')
            pretty_print_tracks(queue_liked_tracks, indent=' '*4, enum=True)
            print()

            choice = get_user_choice('Unlike?')
            if choice == 'yes':
                spotify_liked.remove(queue_liked_tracks, prompt=False)
                spotify_liked.write()

    spotify_queue.write()

    return


def queue_maintenance(
        last_track=None,
        promote_source=None,
        promote_target=None,
        method='Liked',
        ref_playlist=None
):
    if last_track is None:
        if promote_source is not None or promote_target is not None:
            raise ValueError('promote_source or promote_target is specified without last_track')
    else:
        if promote_source is not None and promote_target is not None:
            promote_source_level = djlib_config.get_spotify_queue_level(promote_source)
            promote_target_level = djlib_config.get_spotify_queue_level(promote_target)

            if promote_source_level is not None and promote_target_level is not None:
                if promote_target_level != promote_source_level + 1:
                    raise ValueError(f"Promote queue '{promote_source}' is at level {promote_source_level} "
                                     f"but promote target '{promote_target}' is at level {promote_target_level}")

        else:
            if promote_source is None:
                promote_source = djlib_config.get_default_spotify_queue_at_level(1)
            if promote_target is None:
                promote_source_level = djlib_config.get_spotify_queue_level(promote_source)
                if promote_source_level is None:
                    choice = get_user_choice(f"Unknown source queue {promote_source}; assume it's level 1?")
                    if choice == 'yes':
                        promote_source_level = 1
                    else:
                        print(f"Cannot determine the level of promote source {promote_source}; quitting.")
                        return

                promote_target = djlib_config.get_default_spotify_queue_at_level(promote_source_level+1)

    # Sanity check! Queue and listening history must be disjoint
    sanity_check_disk_queues()

    for i, level in enumerate(djlib_config.spotify_queues):
        for spotify_queue in level:
            sanity_check_spotify_queue(spotify_queue,
                                       is_level_1=(i==0),
                                       is_promote_queue=(spotify_queue == promote_source))

    sys.stdout.flush()

    if last_track is not None:
        promote_tracks_in_spotify_queue(
            last_track,
            promote_source,
            promote_target,
            method=method,
            ref_playlist=ref_playlist
        )

    shazam = SpotifyPlaylist('My Shazam Tracks', create=True)
    shazam_staging = SpotifyPlaylist('Shazam Staging', create=True)

    if len(shazam_staging) > 0:
        choice = get_user_choice(f'Move {len(shazam_staging)} tracks from Shazam Staging to L2 queue?')
        if choice == 'yes':
            l2_queue_name = djlib_config.get_default_spotify_queue_at_level(2)
            l2_queue = SpotifyPlaylist(l2_queue_name)
            l2_queue.append(shazam_staging, prompt=False)
            shazam_staging.truncate(prompt=False)

            l2_queue.write()
            shazam_staging.write()

            sanity_check_spotify_queue(l2_queue_name, is_level_1=False)

    if len(shazam) > 0:
        choice = get_user_choice(f'Move {len(shazam)} tracks from My Shazam Tracks to Shazam Staging?')
        if choice == 'yes':
            shazam_staging.append(shazam, prompt=False)
            shazam.truncate(prompt=False)

            shazam_staging.write()
            shazam.write()

            sanity_check_spotify_queue('Shazam Staging', is_level_1=True)

    return

def pretty_print_spotify_playlist(playlist_name, *, enum=True, liked_only=False):
    spotify_playlist = SpotifyPlaylist(playlist_name)

    if liked_only:
        liked = SpotifyLiked()

        tracks = spotify_playlist.get_intersection(liked)

        print(f"Spotify playlist '{playlist_name}': {len(spotify_playlist)} tracks, {len(tracks)} liked tracks")
    else:
        print(f"Spotify playlist '{playlist_name}': {len(spotify_playlist)} tracks")
        tracks = spotify_playlist.get_df()

    pretty_print_tracks(tracks, enum=enum, ids=False)
    return

def shuffle_spotify_playlist(playlist_name):
    playlist = SpotifyPlaylist(playlist_name, overwrite=True)

    tracks = playlist.get_df()

    new_tracks_idx = random.sample(tracks.index.to_list(), k=len(tracks))

    new_tracks = tracks.loc[new_tracks_idx]

    playlist.set_df(new_tracks)
    playlist.write()

    return

def add_to_queue(tracks):
    """Adds tracks to the disk queue. The tracks have to be either a Container or a DataFrame or
       a string that's the name of a Spotify playlist."""

    if isinstance(tracks, str):
        tracks = SpotifyPlaylist(tracks)
    elif isinstance(tracks, pd.DataFrame):
        tracks = Wrapper(tracks)

    print(f'Attempting to add {len(tracks)} tracks to the disk queue...')

    if len(tracks) == 0:
        return

    listening_history = ListeningHistory()

    listening_history.filter(tracks)

    if len(tracks) == 0:
        return

    queue = Queue()
    queue.append(tracks)
    queue.write()

    choice = get_user_choice('Add to L1 queue also?')
    if choice == 'yes':
        l1_queue_name = djlib_config.get_default_spotify_queue_at_level(1)

        l1_queue = SpotifyPlaylist(l1_queue_name)
        l1_queue.append(tracks, prompt=False)
        l1_queue.write()

    return

def filter_spotify_playlist(playlist_name):
    """Removes tracks in queue and listening history from a Spotify playlist"""

    playlist = SpotifyPlaylist(playlist_name)

    listening_history = ListeningHistory()
    queue = Queue()

    listening_history.filter(playlist)
    playlist.remove(queue)
    playlist.write()

    return


def sample_artist_to_queue(
        *,
        artist_id=None,
        artist_name=None,
        latest=10,
        total=10,
        latest_cutoff_days=365
    ):
    if total <= 0:
        raise ValueError(f'Invalid total tracks {total} for artist {artist_name}')

    print(f'Sampling artist {artist_name} to queue...')

    discography = spotify_discography.get_instance()

    artist_discography = Wrapper(
        contents=discography.get_artist_discography(artist_id=artist_id,
                                                    artist_name=artist_name,
                                                    deduplicate_tracks=True),
        name=f'discography for {artist_name}')

    print(f'Found {len(artist_discography)} tracks')

    listening_history = ListeningHistory()
    queue = Queue()

    listening_history.filter(artist_discography, prompt=False, silent=True)
    artist_discography.remove(queue, prompt=False, silent=True)

    print(f'Left after removing listening history and queue: {len(artist_discography)} tracks')

    if latest == -1:
        latest = sys.maxsize

    max_latest = min(latest, total, len(artist_discography))

    if max_latest != 0:
        latest_cutoff_date = (pd.Timestamp.utcnow() -
                              pd.Timedelta(value=latest_cutoff_days, unit='days'))

        latest_tracks = artist_discography.get_filtered(
            lambda t: t['release_date'] >= latest_cutoff_date
        )

        latest_tracks.sort_values(by='release_date', ascending=False, axis=0, inplace=True)
        latest_tracks = latest_tracks[:max_latest]

        artist_discography.remove(latest_tracks, prompt=False)

        print('Latest tracks:')
        pretty_print_tracks(latest_tracks, indent=' '*4, enum=True, extra_attribs='release_date')

        queue.append(latest_tracks, prompt=False)
        queue.write()

        remaining = min(total - len(latest_tracks), len(artist_discography))
    else:
        remaining = min(total, len(artist_discography))

    if remaining > 0:
        artist_discography.sort('popularity', ascending=False)

        most_popular_tracks = artist_discography.get_df()[:remaining]

        print('Popular tracks:')
        pretty_print_tracks(most_popular_tracks, indent=' '*4, enum=True, extra_attribs='popularity')

        queue.append(most_popular_tracks, prompt=False)
        queue.write()

    return

def text_file_to_spotify_playlist(text_file, target_playlist_name='tmp queue'):
    if target_playlist_name is None:
        target_playlist = None
    else:
        target_playlist = SpotifyPlaylist(target_playlist_name)

    lines = read_lines_from_file(text_file)

    print(f'Looking for {len(lines)} lines of text in Spotify' +
          (f'; adding to playlist {target_playlist_name}'
           if target_playlist_name is not None else ''))

    unmatched_lines = []
    for line in lines:
        spotify_track = text_to_spotify_track(line)

        if spotify_track is None:
            unmatched_lines.append(line)
        elif target_playlist is not None:
            # this avoids a Pandas warning
            spotify_track['added_at'] = pd.Timestamp.now()

            if len(target_playlist.get_df()) == 0:
                target_playlist.set_df(series_to_dataframe(spotify_track))
            else:
                target_playlist.get_df().loc[spotify_track['spotify_id']] = spotify_track


    target_playlist.write(force=True)

    if len(unmatched_lines) == 0:
        print(f'Matched all {len(lines)} lines of text in Spotify')
    else:
        print(f'{len(unmatched_lines)} out of {len(lines)} were left unmatched:')
        for unmatched_line in unmatched_lines:
            print('    ' + unmatched_line)

    return

def remove_artist_from_queue(artist_name):
    queue = Queue()

    if len(queue) == 0:
        return

    tracks_to_remove_bool = queue.get_df().apply(
        lambda track: artist_name in track['artist_names'].split('|'),
        axis=1
    )

    choice = get_user_choice(
        prompt=f'Remove {tracks_to_remove_bool.sum()} tracks from artist {artist_name} from queue?',
        options=['proceed', 'show', 'abort']
    )

    if choice == 'show':
        tracks_to_remove = queue.get_df().loc[tracks_to_remove_bool]
        pretty_print_tracks(tracks_to_remove, enum=True)
        choice = get_user_choice('Proceed?')
        if choice != 'yes':
            return
    elif choice == 'abort':
        return

    queue.set_df(queue.get_df()[~tracks_to_remove_bool])

    print(f'Queue now has {len(queue)} tracks.')

    queue.write(force=True)
    return

def spotify_playlist_from_rekordbox_playlist(spotify_playlist_name, rekordbox_playlist_name):
    spotify_playlist = SpotifyPlaylist(spotify_playlist_name, create=True, overwrite=True)

    rekordbox_playlist = RekordboxPlaylist(rekordbox_playlist_name)

    if spotify_playlist.exists():
        spotify_playlist.truncate()

    spotify_playlist.append(rekordbox_playlist)
    spotify_playlist.write()

    return

def unlike_spotify_playlist(spotify_playlist_name):
    spotify_playlist = SpotifyPlaylist(spotify_playlist_name)

    spotify_liked = SpotifyLiked()

    spotify_liked.remove(spotify_playlist)
    spotify_liked.write()

    return

def review_maintenance(
        *,
        review_playlist,
        next_level_playlist,
        last_track
):
    review_playlist = SpotifyPlaylist(review_playlist)
    next_level_playlist = SpotifyPlaylist(next_level_playlist)

    listened_tracks = get_playlist_listened_tracks(review_playlist, last_track)

    print(f'{review_playlist.get_name()}: {len(listened_tracks)} listened_tracks')
    pretty_print_tracks(listened_tracks, indent=' '*4, enum=True)

    choice = get_user_choice('Is this correct?')
    if choice != 'yes':
        return

    if len(listened_tracks) == 0:
        return

    # find how many of the listened tracks are liked
    liked = SpotifyLiked()

    listened_liked_tracks_idx = listened_tracks.index.intersection(liked.get_df().index, sort=False)
    listened_liked_tracks = listened_tracks.loc[listened_liked_tracks_idx]

    print(f'{review_playlist.get_name()}: {len(listened_liked_tracks)} of the '
          f'{len(listened_tracks)} listened tracks are liked')
    pretty_print_tracks(listened_liked_tracks, indent=' ' * 4, enum=True)
    choice = get_user_choice('Is this correct?')
    if choice != 'yes':
        return
    print()

    if len(listened_liked_tracks) > 0:
        next_level_playlist.append(listened_liked_tracks, prompt=False)
        next_level_playlist.write()

        liked.remove(listened_liked_tracks, prompt=False)
        liked.write()

    listened_not_liked_tracks_idx = listened_tracks.index.difference(listened_liked_tracks_idx)
    listened_not_liked_tracks = listened_tracks.loc[listened_not_liked_tracks_idx]

    print(f'Classifying {len(listened_not_liked_tracks)} non-liked listened tracks as C...')

    library_scripts.reclassify_tracks_as(listened_not_liked_tracks, 'C')

    review_playlist.remove(listened_tracks)
    review_playlist.write()

    return

def queue_stats(start_date, end_date):
    start_date = pd.Timestamp(start_date, tz='UTC')
    end_date = pd.Timestamp(end_date, tz='UTC')

    djlib = Doc('djlib')
    listening_history = ListeningHistory()

    ab_tracks = classification.filter_tracks(
        djlib.get_df(),
        classes=['A', 'B']
    )

    ab_tracks_with_spotify = add_spotify_fields_to_rekordbox(ab_tracks)

    listened_tracks = listening_history.get_filtered(
        lambda track: track['added_at'] >= start_date and track['added_at'] <= end_date
    )

    listened_ab_tracks = listened_tracks.index.intersection(ab_tracks_with_spotify.spotify_id, sort=False)

    return len(listened_tracks), len(listened_ab_tracks)


def review_maintenance(
        playlist,
        ref_playlist=None,
        first_track=None,
        last_track=None,
        method=None
):
    playlist_tracks = SpotifyPlaylist(playlist)
    ref_playlist_tracks = SpotifyPlaylist(ref_playlist) if ref_playlist is not None else None

    listened_tracks = playlist_tracks.slice(
        from_index=first_track,
        to_index=last_track,
        index_column='name',
        ignore_case=True,
        use_prefix=True,
        unambiguous_prefix=True
    )

    print(f'{playlist}: {len(listened_tracks)} listened tracks')
    pretty_print_tracks(listened_tracks, indent=' ' * 4, enum=True)
    choice = get_user_choice('Is this correct?')
    if choice != 'yes':
        return
    print()

    if len(listened_tracks) == 0:
        return

    chosen_tracks, not_chosen_tracks = separate_chosen_tracks(
        listened_tracks,
        method=method,
        reference_tracks=ref_playlist_tracks
    )

    print(f'{playlist}: {len(not_chosen_tracks)} were not chosen')
    pretty_print_tracks(not_chosen_tracks, indent=' ' * 4, enum=True)
    choice = get_user_choice('Is this correct?')
    if choice != 'yes':
        return

    playlist_tracks.remove(listened_tracks, prompt=False)
    playlist_tracks.write()

    choice = get_user_choice('What to do?',
                             options=['Nothing', 'C class', 'D class'])
    if choice == 'Nothing':
        return
    elif choice == 'C class':
        new_class = 'C'
    elif choice == 'D class':
        new_class = 'D'

    not_chosen_tracks_rb = library_scripts.add_rekordbox_fields_to_spotify(
        not_chosen_tracks, drop_missing_ids=True)

    not_chosen_tracks_rb.set_index('rekordbox_id', inplace=True)

    library_scripts.reclassify_tracks_as(not_chosen_tracks_rb, new_class)

    return







