
import random

from utils import *

def _get_library_tracks():
    """A quick and dirty way to get tracks in the Rekordbox library: read rekordbox_to_spotify,
       throw away the rekordbox part and reindex"""

    rekordbox_to_spotify_tracks = docs['rekordbox_to_spotify'].read()

    library_tracks = rekordbox_to_spotify_tracks[
        rekordbox_to_spotify_tracks.columns.drop('rekordbox_id')
    ].rename(columns={'spotify_id': 'id'}
             ).set_index(keys='id', drop=False)

    return library_tracks

def sanity_check_disk_queues():
    queue_tracks = docs['queue'].read()
    print(f'Queue: {len(queue_tracks)} tracks')

    queue_history_tracks = docs['queue_history'].read()
    print(f'Queue history: {len(queue_history_tracks)} tracks')

    # entries in the queue should be unique
    dup_pos = dataframe_duplicate_index_labels(queue_tracks)
    if len(dup_pos) > 0:
        print(f'WARNING: Queue has {len(dup_pos)} duplicate tracks!')
        pretty_print_tracks(queue_tracks.iloc[dup_pos], indent=' '*4, enum=True)
        choice = get_user_choice('Remove?')
        if choice == 'yes':
            queue_tracks = dataframe_drop_rows_at_positions(queue_tracks, dup_pos)
            docs['queue'].write(queue_tracks)

            print(f'Queue now has {len(queue_tracks)} tracks')

    # entries in queue history should be unique
    dup_pos = dataframe_duplicate_index_labels(queue_history_tracks)
    if len(dup_pos) > 0:
        print(f'WARNING: Queue history has {len(dup_pos)} duplicate tracks!')
        pretty_print_tracks(queue_history_tracks.iloc[dup_pos], indent=' ' * 4, enum=True)
        choice = get_user_choice('Remove?')
        if choice == 'yes':
            queue_history_tracks = dataframe_drop_rows_at_positions(queue_history_tracks, dup_pos)
            docs['queue_history'].write(queue_history_tracks)

            print(f'Queue history now has {len(queue_history_tracks)} tracks')

    # queue tracks should not be in queue history
    queue_tracks_in_queue_history_idx = queue_tracks.index.intersection(queue_history_tracks.index, sort=False)
    if len(queue_tracks_in_queue_history_idx) > 0:
        print(f'WARNING: {len(queue_tracks_in_queue_history_idx)} queue tracks are in queue history.')
        pretty_print_tracks(
            queue_tracks.loc[queue_tracks_in_queue_history_idx],
            indent=' '*4,
            enum=True
        )
        choice = get_user_choice('Remove?')

        if choice == 'yes':
            queue_tracks = queue_tracks.loc[queue_tracks.index.difference(queue_history_tracks.index, sort=False)]

            docs['queue'].write(queue_tracks)

            print(f'Queue now has {len(queue_tracks)} tracks')

    # library tracks should be in queue history and should not be in queue
    library_tracks = _get_library_tracks()

    library_tracks_not_in_queue_history_idx = library_tracks.index.difference(queue_history_tracks.index, sort=False)

    add_to_doc('queue_history', 'library', library_tracks)

    print()

    return


def remove_from_queue(listened_tracks=[], warn_for_missing=True):
    queue_tracks = docs['queue'].read()
    print(f'Queue: {len(queue_tracks)} tracks')

    remaining_queue_tracks_idx = queue_tracks.index.difference(listened_tracks.index, sort=False)

    if warn_for_missing and len(remaining_queue_tracks_idx) + len(listened_tracks) > len(queue_tracks):
        print(
            f'WARNING: '
            f'{len(remaining_queue_tracks_idx) + len(listened_tracks) - len(queue_tracks)} '
            f'listened tracks were already removed from the queue')
        choice = get_user_choice('Continue?')
        if choice != 'yes':
            return
    elif len(remaining_queue_tracks_idx) + len(listened_tracks) < len(queue_tracks):
        raise Exception('Something strange is happening!')

    if len(remaining_queue_tracks_idx) == len(queue_tracks):
        print('None of these tracks are in queue; nothing to do')
        return

    queue_tracks = queue_tracks.loc[remaining_queue_tracks_idx]
    docs['queue'].write(queue_tracks)
    print(f'Queue now has {len(queue_tracks)} tracks')

    return


def get_playlist_listened_tracks(
        playlist_name=None,
        playlist_id=None,
        playlist_tracks=None,
        last_listened_track=None):

    if playlist_tracks is None:
        if playlist_id is None:
            if playlist_name is None:
                raise Exception("At least one of playlist_tracks, playlist_id or playlist_name must be specified")

            playlist_id = spotify.get_playlist_id(playlist_name)
            playlist_tracks = spotify.get_playlist_tracks(playlist_id)

    if last_listened_track is not None:
        if isinstance(last_listened_track, int):
            if last_listened_track > len(playlist_tracks):
                raise Exception(
                    f'Playlist last listened track is {last_listened_track}; '
                    f'playlist has only {len(playlist_tracks)} tracks')

            return playlist_tracks.iloc[:last_listened_track]

        if isinstance(last_listened_track, str):
            if last_listened_track.upper() == 'ALL':
                return playlist_tracks

            last_listened_track_idx = None

            for i in range(len(playlist_tracks)):
                if playlist_tracks.iloc[i]['name'].upper() == last_listened_track.upper():
                    last_listened_track_idx = i
                    break

            if last_listened_track_idx is None:
                # Try to find it as a prefix
                for i in range(len(playlist_tracks)):
                    if playlist_tracks.iloc[i]['name'].upper().startswith(last_listened_track.upper()):
                        last_listened_track_idx = i
                        break
                if last_listened_track_idx is None:
                    raise Exception(f"Track '{last_listened_track}' not found in playlist")

            return playlist_tracks.iloc[:(last_listened_track_idx+1)]

        raise Exception(f"Invalid type for last_listened_track: {type(last_listened_track)}")

    print('WARNING: Attempting to determine listened tracks through the listening history; this may miss some tracks.')
    choice = get_user_choice('Continue?')
    if choice != 'yes':
        return pd.DataFrame([])

    listened_tracks = spotify.get_recently_played_tracks()

    return playlist_tracks.loc[playlist_tracks.index.intersection(listened_tracks.index, sort=False)]

_DEFAULT_L1_QUEUE = 'L1 queue'
_DEFAULT_L2_QUEUE = 'L2 queue'

def move_l1_queue_listened_tracks_to_l2(
        l1_queue_name=_DEFAULT_L1_QUEUE,
        l2_queue_name=_DEFAULT_L2_QUEUE,
        l1_queue_last_listened_track=None):

    l1_queue_id = spotify.get_playlist_id(l1_queue_name)
    l2_queue_id = spotify.get_playlist_id(l2_queue_name)

    l1_queue_tracks = spotify.get_playlist_tracks(l1_queue_id)
    print(f'{l1_queue_name}: {len(l1_queue_tracks)} tracks')

    if l1_queue_tracks.empty:
        return l1_queue_tracks, None, None

    l2_queue_tracks = spotify.get_playlist_tracks(l2_queue_id)
    print(f'{l2_queue_name}: {len(l2_queue_tracks)} tracks')

    listened_tracks = get_playlist_listened_tracks(
        playlist_tracks=l1_queue_tracks,
        last_listened_track=l1_queue_last_listened_track)

    print(f'{l1_queue_name}: {len(listened_tracks)} listened tracks')
    pretty_print_tracks(listened_tracks, indent=' ' * 4, enum=True)
    print()
    choice = get_user_choice('Is this correct?')
    if choice != 'yes':
        return

    if len(listened_tracks) == 0:
        return

    # find how many of the listened tracks are liked
    liked_tracks = spotify.get_liked_tracks()
    print(f'Spotify Liked Tracks: {len(liked_tracks)} tracks')

    listened_liked_tracks_idx = listened_tracks.index.intersection(liked_tracks.index, sort=False)
    print(f'{l1_queue_name}: {len(listened_liked_tracks_idx)} of the '
          f'{len(listened_tracks)} listened tracks are liked')
    pretty_print_tracks(listened_tracks.loc[listened_liked_tracks_idx], indent=' ' * 4, enum=True)
    print()

    if len(listened_liked_tracks_idx) > 0:
        listened_liked_tracks_not_in_l2_idx = listened_liked_tracks_idx.difference(l2_queue_tracks.index,
                                                                                   sort=False)
        if len(listened_liked_tracks_not_in_l2_idx) < len(listened_liked_tracks_idx):
            print(
                f'WARNING: '
                f'{len(listened_liked_tracks_idx) - len(listened_liked_tracks_not_in_l2_idx)} '
                f'of these tracks are already in {l2_queue_name}')
            choice = get_user_choice('Continue?')
            if choice != 'yes':
                return

        if len(listened_liked_tracks_not_in_l2_idx) > 0:
            choice = get_user_choice(f'Add {len(listened_liked_tracks_not_in_l2_idx)} tracks to {l2_queue_name} ?')
            if choice == 'yes':
                spotify.add_tracks_to_playlist(l2_queue_id, listened_liked_tracks_not_in_l2_idx,
                                                   # avoid duplicate check since we've already done it
                                                   check_for_duplicates=False)

                l2_queue_tracks = pd.concat([l2_queue_tracks, l1_queue_tracks.loc[listened_liked_tracks_not_in_l2_idx]])
                print(f'{l2_queue_name} now has {len(l2_queue_tracks)} tracks')

    add_to_doc('queue_history', 'listened tracks', listened_tracks)

    choice = get_user_choice(f'Remove {len(listened_tracks)} listened tracks from queue?')
    if choice == 'yes':
        remove_from_queue(listened_tracks)

    choice = get_user_choice(f'Remove {len(listened_tracks)} listened tracks from {l1_queue_name}?')
    if choice == 'yes':
        spotify.remove_tracks_from_playlist(l1_queue_id, listened_tracks.index)
        l1_queue_tracks = l1_queue_tracks.loc[l1_queue_tracks.index.difference(listened_tracks.index, sort=False)]
        print(f'{l1_queue_name} now has {len(l1_queue_tracks)} tracks')

    return

def replenish_l1_queue(
        l1_queue_name=_DEFAULT_L1_QUEUE,
        l1_queue_id=None,
        l1_queue_tracks=None,
        target_size=200):

    if l1_queue_tracks is None:
        if l1_queue_id is None:
            l1_queue_id = spotify.get_playlist_id(l1_queue_name)

        l1_queue_tracks = spotify.get_playlist_tracks(l1_queue_id)

    if target_size is None or len(l1_queue_tracks) >= target_size:
        return l1_queue_tracks

    tracks_to_add = target_size - len(l1_queue_tracks)

    # only replenish the default L1 queue - not random playlists - from the disk queue
    # if l1_queue_name == DEFAULT_L1_QUEUE:
    if True:
        choice = get_user_choice(f'Add up to {tracks_to_add} new tracks to {l1_queue_name}?')
        if choice == 'yes':
            queue_tracks = docs['queue'].read()
            print(f'Queue: {len(queue_tracks)} tracks')

            queue_tracks_not_in_l1_queue_idx = queue_tracks.index.difference(l1_queue_tracks.index, sort=False)

            num_tracks_to_add = min(target_size - len(l1_queue_tracks), len(queue_tracks_not_in_l1_queue_idx))

            if num_tracks_to_add < len(queue_tracks_not_in_l1_queue_idx):
                tracks_to_add_idx = random.sample(queue_tracks_not_in_l1_queue_idx.to_list(), k=num_tracks_to_add)
                tracks_to_add = queue_tracks.loc[tracks_to_add_idx]
            else:
                tracks_to_add = queue_tracks.loc[queue_tracks_not_in_l1_queue_idx]

            print(f'Adding {num_tracks_to_add} tracks to {l1_queue_name}')
            pretty_print_tracks(tracks_to_add, indent=' '*4, enum=True)

            spotify.add_tracks_to_playlist(l1_queue_id, tracks_to_add.index,
                                               # skip the duplicate check since we've already done it
                                               check_for_duplicates=False)

            l1_queue_tracks = pd.concat([l1_queue_tracks, tracks_to_add])

            print(f'{l1_queue_name} now has {len(l1_queue_tracks)} tracks')

    return

def sanity_check_l1_queue(
        l1_queue_name=_DEFAULT_L1_QUEUE,
        l1_queue_id=None):

    if l1_queue_id is None:
        l1_queue_id = spotify.get_playlist_id(l1_queue_name)

    l1_queue_tracks = spotify.get_playlist_tracks(l1_queue_id)
    print(f'{l1_queue_name}: {len(l1_queue_tracks)} tracks')

    queue_tracks = docs['queue'].read()

    # Make sure all items in the L1 queue are also in the disk queue
    l1_queue_tracks_not_in_queue_idx = l1_queue_tracks.index.difference(queue_tracks.index, sort=False)
    if len(l1_queue_tracks_not_in_queue_idx) > 0:
        print(f'WARNING: {len(l1_queue_tracks_not_in_queue_idx)} tracks are in {l1_queue_name} but not in disk queue')
        pretty_print_tracks(l1_queue_tracks.loc[l1_queue_tracks_not_in_queue_idx])

        choice = get_user_choice('Remove?')
        if choice == 'yes':
            spotify.remove_tracks_from_playlist(l1_queue_id, l1_queue_tracks_not_in_queue_idx)

            l1_queue_tracks = l1_queue_tracks.loc[l1_queue_tracks.index.difference(l1_queue_tracks_not_in_queue_idx, sort=False)]

            print(f'{l1_queue_name} now has {len(l1_queue_tracks)} tracks')

    # Make sure items in the L1 queue are unique
    dup_pos = dataframe_duplicate_index_labels(l1_queue_tracks)
    if len(dup_pos) > 0:
        print(f'WARNING: {l1_queue_name} has {len(dup_pos)} duplicate tracks!')
        pretty_print_tracks(l1_queue_tracks.iloc[dup_pos])

        choice = get_user_choice('Remove?')
        if choice == 'yes':
            spotify.remove_tracks_from_playlist(l1_queue_id, l1_queue_tracks.index[dup_pos])

            l1_queue_tracks = dataframe_drop_rows_at_positions(l1_queue_tracks, dup_pos)

            print(f'{l1_queue_name} now has {len(l1_queue_tracks)} tracks')

    # Make sure all items in the L1 queue are not liked
    liked_tracks = spotify.get_liked_tracks()

    l1_queue_liked_tracks_idx = l1_queue_tracks.index.intersection(liked_tracks.index, sort=False)

    if len(l1_queue_liked_tracks_idx) > 0:
        print(f'WARNING: {len(l1_queue_liked_tracks_idx)} {l1_queue_name} tracks are already liked')
        pretty_print_tracks(l1_queue_tracks.loc[l1_queue_liked_tracks_idx], indent=' '*4, enum=True)
        print()

        choice = get_user_choice('Unlike?')
        if choice == 'yes':
            spotify.remove_liked_tracks(l1_queue_liked_tracks_idx)

    return


def add_to_l2_queue(
        tracks_name,
        tracks,
        l2_queue_name=_DEFAULT_L2_QUEUE):

    tracks = dataframe_ensure_unique_index(tracks)
    print(f'Adding {len(tracks)} {tracks_name} tracks to {l2_queue_name}')

    if len(tracks) == 0:
        return

    l2_queue_id = spotify.get_playlist_id(l2_queue_name)

    l2_queue_tracks = spotify.get_playlist_tracks(l2_queue_id)
    print(f'{l2_queue_name}: {len(l2_queue_tracks)} tracks')

    new_tracks_idx = tracks.index.difference(l2_queue_tracks.index, sort=False)

    print(f'{len(tracks) - len(new_tracks_idx)} tracks are already in {l2_queue_name}; '
          f'adding remaining {len(new_tracks_idx)} tracks')
    new_tracks = tracks.loc[new_tracks_idx]
    pretty_print_tracks(new_tracks)

    spotify.add_tracks_to_playlist(l2_queue_id, new_tracks_idx, check_for_duplicates=False)

    add_to_doc('queue_history', tracks_name, tracks)

    choice = get_user_choice('Remove tracks from queue?')
    if choice == 'yes':
        remove_from_queue(new_tracks, warn_for_missing=False)

    return

def add_shazam_to_l2_queue(
        shazam_name = 'My Shazam Tracks',
        shazam_id=None,
        l2_queue_name=_DEFAULT_L2_QUEUE):

    if shazam_id is None:
        shazam_id = spotify.get_playlist_id(shazam_name)

    shazam_tracks = spotify.get_playlist_tracks(shazam_id)

    shazam_tracks = dataframe_ensure_unique_index(shazam_tracks)

    print(f"'{shazam_name}' playlist: {len(shazam_tracks)} tracks")
    pretty_print_tracks(shazam_tracks, indent=' '*4, enum=True)

    if len(shazam_tracks) == 0:
        return

    choice = get_user_choice(f"Add to {l2_queue_name}?")
    if choice != 'yes':
        return

    add_to_l2_queue(
        shazam_tracks,
        l2_queue_name=l2_queue_name)

    choice = get_user_choice(f"Remove tracks from '{shazam_name}'?")
    if choice == 'yes':
        spotify.remove_tracks_from_playlist(shazam_id, shazam_tracks.index)

    return


def sanity_check_l2_queue(
        l2_queue_name=_DEFAULT_L2_QUEUE,
        l2_queue_id=None,
):

    if l2_queue_id is None:
        l2_queue_id = spotify.get_playlist_id(l2_queue_name)

    l2_queue_tracks = spotify.get_playlist_tracks(l2_queue_id)
    print(f'{l2_queue_name}: {len(l2_queue_tracks)} tracks')

    # Make sure all items in the L2 queue are already liked
    liked_tracks = spotify.get_liked_tracks()
    print(f'Spotify Liked Tracks: {len(liked_tracks)} tracks')

    l2_queue_unliked_tracks_idx = l2_queue_tracks.index.difference(liked_tracks.index, sort=False)

    if len(l2_queue_unliked_tracks_idx) > 0:
        print(f'{len(l2_queue_unliked_tracks_idx)} {l2_queue_name} tracks are not in Liked Tracks')
        l2_queue_unliked_tracks = l2_queue_tracks.loc[l2_queue_unliked_tracks_idx]
        pretty_print_tracks(l2_queue_unliked_tracks)
        choice = get_user_choice('Add?')
        if choice == 'yes':
            spotify.add_liked_tracks(l2_queue_unliked_tracks_idx)

            liked_tracks = pd.concat([liked_tracks, l2_queue_unliked_tracks])

    # Make sure all items in the L2 queue are in queue_history
    queue_history = docs['queue_history'].read()

    l2_queue_not_in_queue_history_idx = l2_queue_tracks.index.difference(queue_history.index)

    if len(l2_queue_not_in_queue_history_idx) > 0:
        l2_queue_not_in_queue_history = l2_queue_tracks.loc[l2_queue_not_in_queue_history_idx]

        print(f'{len(l2_queue_not_in_queue_history_idx)} {l2_queue_name} tracks are not in queue history')
        pretty_print_tracks(l2_queue_not_in_queue_history)
        choice = get_user_choice('Add?')

        if choice == 'yes':
            queue_history = pd.concat([queue_history, l2_queue_not_in_queue_history])
            docs['queue_history'].write(queue_history)

    # Make sure all items in the L2 queue are not in queue
    queue = docs['queue'].read()

    l2_queue_in_queue_idx = l2_queue_tracks.index.intersection(queue.index)

    if len(l2_queue_in_queue_idx) > 0:
        l2_queue_in_queue = l2_queue_tracks.loc[l2_queue_in_queue_idx]

        print(f'{len(l2_queue_in_queue_idx)} {l2_queue_name} tracks are in the queue')
        pretty_print_tracks(l2_queue_in_queue)
        choice = get_user_choice('Remove?')

        if choice == 'yes':
            queue = queue.loc[queue.index.difference(l2_queue_tracks.index, sort=False)]
            docs['queue'].write(queue)

    return

def queue_maintenance(
        l1_queue_name = _DEFAULT_L1_QUEUE,
        l2_queue_name = _DEFAULT_L2_QUEUE,
        shazam_name = 'My Shazam Tracks',
        last_track = None,
        l1_queue_target_size = 200
):

    # Sanity check! Queue and queue history must be disjoint
    sanity_check_disk_queues()

    move_l1_queue_listened_tracks_to_l2(
        l1_queue_name=l1_queue_name,
        l2_queue_name=l2_queue_name,
        l1_queue_last_listened_track=last_track
    )

    replenish_l1_queue(
        l1_queue_name=l1_queue_name,
        target_size=l1_queue_target_size)

    add_shazam_to_l2_queue(
        shazam_name=shazam_name,
        l2_queue_name=l2_queue_name)

    sanity_check_l1_queue(
        l1_queue_name=l1_queue_name)

    sanity_check_l2_queue(
        l2_queue_name=l2_queue_name)

    return

def pretty_print_spotify_playlist(playlist_name):
    tracks = spotify.get_playlist_tracks(playlist_name)

    print(f"Spotify playlist '{playlist_name}': {len(tracks)} tracks")
    pretty_print_tracks(tracks, enum=True)
    return

def shuffle_spotify_playlist(playlist_name):
    tracks = spotify.get_playlist_tracks(playlist_name)

    new_tracks = random.sample(tracks.index.to_list(), k=len(tracks))

    spotify_playlists = spotify.get_playlists()

    new_playlist_name = playlist_name + ' - shuffled'

    while new_playlist_name in spotify_playlists.index:
        new_playlist_name += ' - shuffled'

    spotify.create_playlist(new_playlist_name)
    spotify.add_tracks_to_playlist(new_playlist_name, new_tracks)

    print(f"Created Spotify playlist '{new_playlist_name}' with {len(new_tracks)} tracks")

    return
