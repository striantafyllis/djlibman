
import classification
import spotify_discography
from library_workflow import add_spotify_fields_to_rekordbox

from spotify_util import *

def sanity_check_disk_queue(disk_queue_name):
    queue = Queue(disk_queue_name)
    print(f'Queue {disk_queue_name}: {len(queue)} tracks')

    listening_history = ListeningHistory()
    print(f'Listening history: {len(listening_history)} tracks')

    library = RekordboxPlaylist('Main Library')
    print(f'Library: {len(library)} tracks')

    # entries in the queue should be unique
    queue.deduplicate()
    queue.deduplicate(function=get_track_signature)

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
        *,
        last_track=None,
        promote_source_name=None,
        promote_target_name=None,
        side_playlist_name=None,
        unambiguous_prefix=True,
        disk_queue=None,
        method='liked',
        unlike=True,
        ref_playlist=None,
        remove_from_source=True,
        add_to_listening_history=False
):
    if side_playlist_name is not None and not remove_from_source:
        raise ValueError("Side playlist is set but remove_from_source is false; this doesn't make sense.")

    promote_source = SpotifyPlaylist(promote_source_name)
    promote_target = SpotifyPlaylist(promote_target_name)

    side_playlist = SpotifyPlaylist(side_playlist_name) if side_playlist_name is not None else None

    if isinstance(last_track, int):
        # because this comes from a human, it's probably 1-based, whereas we want 0-based
        last_track -= 1

    listened_tracks = promote_source.slice(
        from_index=0,
        to_index=last_track,
        unambiguous_prefix=unambiguous_prefix,
        index_column='name',
        ignore_case=True,
        use_prefix=True,
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
        print(f'Appending {len(listened_chosen_tracks)} tracks to playlist {promote_target_name}...')
        promote_target.append(listened_chosen_tracks, prompt=False, silent=True)
        promote_target.write()

        if unlike:
            liked = SpotifyLiked()

            liked.remove(listened_chosen_tracks, prompt=False, silent=True)
            liked.write()

    if len(listened_not_chosen_tracks) > 0 and side_playlist is not None:
        print(f'Appending remaining {len(listened_not_chosen_tracks)} tracks to side playlist {side_playlist_name}...')
        side_playlist.append(listened_not_chosen_tracks, prompt=False, silent=True)
        side_playlist.write()

    if remove_from_source:
        promote_source.remove(listened_tracks, prompt=False, silent=True)
        promote_source.write()

    if disk_queue is not None:
        print(f'Removing listened tracks from disk queue {disk_queue}...')
        queue = Queue(disk_queue)
        queue.remove(listened_tracks, prompt=False, silent=True)
        queue.write()

        print(f'Disk queue {disk_queue} now has {len(queue)} tracks')

    if add_to_listening_history:
        print(f'Adding listened tracks to listening history...')
        listening_history = ListeningHistory()
        listening_history.append(listened_tracks, prompt=False, silent=True)
        listening_history.write()

    return listened_tracks

def replenish_spotify_queue(
        playlist_name='L1 queue',
        queue_name='queue',
        target_size=100):
    spotify_queue = SpotifyPlaylist(playlist_name)
    disk_queue = Queue(name=queue_name)

    tracks_wanted = target_size - len(spotify_queue)
    if tracks_wanted <= 0:
        print(f'Spotify playlist {playlist_name} already has {len(spotify_queue)} tracks; no replenishment needed.')
        return

    candidate_tracks_idx = disk_queue.get_df().index.difference(spotify_queue.get_df().index)

    if len(candidate_tracks_idx) == 0:
        print(f'Disk queue has no other tracks; no replenishment possible.')
        return

    num_tracks_to_add = min(tracks_wanted, len(candidate_tracks_idx))

    choice = get_user_choice(f'Add {tracks_wanted} new tracks to {playlist_name}?')
    if choice == 'yes':
        if num_tracks_to_add < len(candidate_tracks_idx):
            tracks_to_add_idx = random.sample(candidate_tracks_idx.to_list(), k=num_tracks_to_add)
            tracks_to_add = disk_queue.get_df().loc[tracks_to_add_idx]
        else:
            tracks_to_add = disk_queue.get_df().loc[candidate_tracks_idx]

        print(f'Adding {num_tracks_to_add} tracks to {playlist_name}')
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
        *,
        unambiguous_prefix=True,
        disk_queue=None,
        spotify_queues=[],
        promote_source=None,
        promote_target=None,
        side_playlist=None,
        method='Liked',
        ref_playlist=None,
        remove_from_source=True,
        add_to_listening_history=True
):
    # sanity check the spotify_queues argument
    if spotify_queues is not None:
        if spotify_queues is not None and not isinstance(spotify_queues, list):
            raise ValueError('spotify_queues must be a list')
        for i in range(len(spotify_queues)):
            queue = spotify_queues[i]

            if isinstance(queue, list):
                if len(queue) == 0:
                    raise ValueError(f'Spotify queue at level {i+1} is an empty list')
                else:
                    for queue2 in queue:
                        if not isinstance(queue2, str):
                            raise ValueError(f'Spotify queue at level {i+1} contains non-string element {queue2}')
            elif isinstance(queue, str):
                spotify_queues[i] = [queue]
            else:
                raise ValueError(f'Spotify queue at level {i+1} must be a string or a list')

    if last_track is None:
        if promote_source is not None or promote_target is not None:
            raise ValueError('promote_source or promote_target is specified without last_track')
    else:
        if promote_source is None:
            if spotify_queues is None or len(spotify_queues) == 0:
                raise ValueError('promote_source is None and no spotify_queues were specified')
            promote_source = spotify_queues[0][0]
            promote_source_level = 1
        else:
            promote_source_level = None

            for i, queues in enumerate(spotify_queues):
                if promote_source in queues:
                    promote_source_level = i+1
                    break

        if promote_target is None:
            if promote_source_level is None:
                raise ValueError('promote_target is None but promote_source level cannot be determined')

            if spotify_queues is None or len(spotify_queues) < promote_source_level:
                raise ValueError(f'promote_target is None and there are no spotify_queues at level {promote_source_level+1}')

            promote_target = spotify_queues[promote_source_level][0]
            promote_target_level = promote_source_level+1
        else:
            promote_target_level = None

            for i, queue in enumerate(spotify_queues):
                if promote_target in queue:
                    promote_target_level = i + 1
                    break

        if promote_source_level is not None and promote_target_level is not None:
            if promote_target_level != promote_source_level + 1:
                raise ValueError(f"Promote queue '{promote_source}' is at level {promote_source_level} "
                                 f"but promote target '{promote_target}' is at level {promote_target_level}")

    # Sanity check! Queue and listening history must be disjoint
    if disk_queue is not None:
        sanity_check_disk_queue(disk_queue)

    for i, level in enumerate(spotify_queues):
        for spotify_queue in level:
            sanity_check_spotify_queue(spotify_queue,
                                       is_level_1=(i==0),
                                       is_promote_queue=(spotify_queue == promote_source))

    sys.stdout.flush()

    if last_track is not None:
        promote_tracks_in_spotify_queue(
            last_track=last_track,
            promote_source_name=promote_source,
            promote_target_name=promote_target,
            side_playlist_name=side_playlist,
            unambiguous_prefix=unambiguous_prefix,
            disk_queue=disk_queue if promote_source_level==1 else None,
            method=method,
            ref_playlist=ref_playlist,
            remove_from_source=remove_from_source,
            add_to_listening_history=(add_to_listening_history and promote_source_level==1)
        )

    return

def add_to_queue(tracks):
    """Adds tracks to the disk queue. The tracks have to be either a Container or a DataFrame or
       a string that's the name of a Spotify playlist."""

    if isinstance(tracks, str):
        tracks = SpotifyPlaylist(tracks)
    elif isinstance(tracks, pd.DataFrame):
        tracks = ct.Wrapper(tracks)

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

def filter_spotify_playlist(playlist_name, queue_name=None):
    """Removes tracks in queue and listening history from a Spotify playlist"""

    playlist = SpotifyPlaylist(playlist_name)

    listening_history = ListeningHistory()
    listening_history.filter(playlist)

    if queue_name is not None:
        queue = Queue(queue_name)
        playlist.remove(queue)

    playlist.write()

    return

def remove_artist_from_queue(artist_name, queue_name):
    queue = Queue(queue_name)

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

def add_unlistened_from_regex_to_playlist(source_regex, target_playlist_name):
    source_playlists = get_spotify_playlists_regex(source_regex)

    print(f"Found {len(source_playlists)} playlists matching regex '{source_regex}'")

    target_playlist = SpotifyPlaylist(target_playlist_name)
    listening_history = ListeningHistory()

    for source_playlist_name in source_playlists:
        source_playlist = SpotifyPlaylist(source_playlist_name)

        num_tracks = len(source_playlist)
        listening_history.filter(source_playlist, prompt=False, silent=True)

        num_unlistened_tracks = len(source_playlist)

        source_playlist.remove(target_playlist, prompt=False, silent=True)

        num_new_tracks = len(source_playlist)

        print(f"Source playlist '{source_playlist_name}': {num_tracks} tracks, "
              f"{num_unlistened_tracks} unlistened tracks, {num_new_tracks} "
              f"new tracks -> adding to playlist '{target_playlist_name}'")
        if num_new_tracks > 0:
            target_playlist.append(source_playlist, prompt=False, silent=True)

    target_playlist.write()
    return


def add_liked_from_regex_to_playlist(source_regex, target_playlist_name):
    source_playlists = get_spotify_playlists_regex(source_regex)

    print(f"Found {len(source_playlists)} playlists matching regex '{source_regex}'")

    target_playlist = SpotifyPlaylist(target_playlist_name)
    liked = SpotifyLiked()

    for source_playlist_name in source_playlists:
        source_playlist = SpotifyPlaylist(source_playlist_name)

        num_tracks = len(source_playlist)

        source_playlist.intersect(liked, prompt=False, silent=True)
        num_liked_tracks = len(source_playlist)

        source_playlist.remove(target_playlist, prompt=False, silent=True)

        num_new_tracks = len(source_playlist)

        print(f"Source playlist '{source_playlist_name}': {num_tracks} tracks, "
              f"{num_liked_tracks} liked tracks, {num_new_tracks} "
              f"new tracks -> adding to playlist '{target_playlist_name}'")
        if num_new_tracks > 0:
            target_playlist.append(source_playlist, prompt=False, silent=True)

    target_playlist.write()
    return
