
import random

from djlib_config import *
from utils import *

def sanity_check_disk_queues(queue_tracks=None, queue_history_tracks=None):
    if queue_tracks is None:
        queue_tracks = docs['queue'].read()
        print('Queue: %d tracks' % len(queue_tracks))
    if queue_history_tracks is None:
        queue_history_tracks = docs['queue_history'].read()
        print('Queue history: %d tracks' % len(queue_history_tracks))

    # entries in the queue should be unique
    dup_pos = dataframe_duplicate_index_labels(queue_tracks)
    if len(dup_pos) > 0:
        print('WARNING: Queue has %d duplicate tracks!' % len(dup_pos))
        pretty_print_tracks(queue_tracks.iloc[dup_pos], indent=' '*4, enum=True)
        choice = get_user_choice('Remove? (y/n)')
        if choice == 'yes':
            queue_tracks = dataframe_drop_rows_at_positions(queue_tracks, dup_pos)
            docs['queue'].write(queue_tracks)

            print('Queue now has %d tracks' % len(queue_tracks))

    # entries in queue history should be unique
    dup_pos = dataframe_duplicate_index_labels(queue_history_tracks)
    if len(dup_pos) > 0:
        print('WARNING: Queue history has %d duplicate tracks!' % len(dup_pos))
        pretty_print_tracks(queue_history_tracks.iloc[dup_pos], indent=' ' * 4, enum=True)
        choice = get_user_choice('Remove? (y/n)')
        if choice == 'yes':
            queue_history_tracks = dataframe_drop_rows_at_positions(queue_history_tracks, dup_pos)
            docs['queue_history'].write(queue_history_tracks)

            print('Queue history now has %d tracks' % len(queue_history_tracks))

    # queue tracks should not be in queue history
    queue_tracks_in_queue_history_idx = queue_tracks.index.intersection(queue_history_tracks.index, sort=False)
    if len(queue_tracks_in_queue_history_idx) > 0:
        print('WARNING: %d queue tracks are in queue history.' % len(queue_tracks_in_queue_history_idx))
        pretty_print_tracks(
            queue_tracks.loc[queue_tracks_in_queue_history_idx],
            indent=' '*4,
            enum=True
        )
        choice = get_user_choice('Remove?')

        if choice == 'yes':
            queue_tracks = queue_tracks.loc[queue_tracks.index.difference(queue_history_tracks.index, sort=False)]

            docs['queue'].write(queue_tracks)

            print('Queue now has %d tracks' % len(queue_tracks))

    print()

    return


def add_to_queue_history(new_tracks):
    if len(new_tracks) == 0:
        return

    queue_history = docs['queue_history']
    queue_history_tracks = queue_history.read()
    print('Queue history: %d tracks' % len(queue_history_tracks))

    if queue_history_tracks.index.name != 'id':
        raise Exception('queue_history not indexed by ID')

    if not isinstance(new_tracks, pd.DataFrame):
        new_tracks = pd.DataFrame(new_tracks, columns=queue_history.columns)
        new_tracks = new_tracks.set_index(new_tracks[queue_history.index.name])
    else:
        for column in queue_history_tracks.columns:
            if column not in new_tracks.columns:
                raise Exception("New items are missing column '%s'" % column)

    # this takes care of extra columns, columns in different order etc.
    new_tracks = new_tracks[queue_history_tracks.columns]

    if new_tracks.index.name == 'id':
        new_history_ids = new_tracks.index
    else:
        new_history_ids = pd.Index(new_tracks.id)

    unique_new_ids = new_history_ids.difference(queue_history_tracks.index, sort=False)

    print('Received %d new items, of which %d already exist; adding %d new items' % (
        len(new_tracks),
        len(new_tracks) - len(unique_new_ids),
        len(unique_new_ids)))

    new_history = pd.concat([queue_history_tracks, new_tracks.loc[unique_new_ids]])
    queue_history.write(new_history)
    print('Queue history now has %d tracks' % len(new_history))

    return


def remove_from_queue(listened_tracks=[], warn_for_missing=True):
    queue_tracks = docs['queue'].read()
    print('Queue: %d tracks' % len(queue_tracks))

    remaining_queue_tracks_idx = queue_tracks.index.difference(listened_tracks.index, sort=False)

    if warn_for_missing and len(remaining_queue_tracks_idx) + len(listened_tracks) > len(queue_tracks):
        print('WARNING: %d listened tracks were already removed from the queue' %
              (len(remaining_queue_tracks_idx) + len(listened_tracks) - len(queue_tracks)))
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
    print('Queue now has %d tracks' % len(queue_tracks))

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
                raise Exception('Playlist last listened track is %d; playlist has only %d tracks' % (
                    last_listened_track,
                    len(playlist_tracks)
                ))

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

        raise Exception("Invalid type for last_listened_track: %s" % type(last_listened_track))

    print('WARNING: Attempting to determine listened tracks through the listening history; this may miss some tracks.')
    choice = get_user_choice('Continue?')
    if choice != 'yes':
        return pd.DataFrame([])

    listened_tracks = spotify.get_recently_played_tracks()

    return playlist_tracks.loc[playlist_tracks.index.intersection(listened_tracks.index, sort=False)]

DEFAULT_L1_QUEUE = 'L1 queue'
DEFAULT_L2_QUEUE = 'L2 queue'

def move_l1_queue_listened_tracks_to_l2(
        l1_queue_name=DEFAULT_L1_QUEUE,
        l2_queue_name=DEFAULT_L2_QUEUE,
        l1_queue_id=None,
        l2_queue_id=None,
        l1_queue_last_listened_track=None):

    if l1_queue_id is None:
        l1_queue_id = spotify.get_playlist_id(l1_queue_name)
    if l2_queue_id is None:
        l2_queue_id = spotify.get_playlist_id(l2_queue_name)

    l1_queue_tracks = spotify.get_playlist_tracks(l1_queue_id)
    print('%s: %d tracks' % (l1_queue_name, len(l1_queue_tracks)))

    if l1_queue_tracks.empty:
        return l1_queue_tracks, None, None

    l2_queue_tracks = spotify.get_playlist_tracks(l2_queue_id)
    print('%s: %d tracks' % (l2_queue_name, len(l2_queue_tracks)))

    listened_tracks = get_playlist_listened_tracks(
        playlist_tracks=l1_queue_tracks,
        last_listened_track=l1_queue_last_listened_track)

    print('%s: %d listened tracks' % (l1_queue_name, len(listened_tracks)))
    pretty_print_tracks(listened_tracks, indent=' ' * 4, enum=True)
    print()
    choice = get_user_choice('Is this correct?')
    if choice != 'yes':
        return l1_queue_tracks, l2_queue_tracks, None

    if len(listened_tracks) == 0:
        return l1_queue_tracks, l2_queue_tracks, None

    # find how many of the listened tracks are liked
    liked_tracks = spotify.get_liked_tracks()
    print('Spotify Liked Tracks: %d tracks' % len(liked_tracks))

    listened_liked_tracks_idx = listened_tracks.index.intersection(liked_tracks.index, sort=False)
    print('%s: %d of the %d listened tracks are liked' % (
        l1_queue_name,
        len(listened_liked_tracks_idx),
        len(listened_tracks)
    ))
    pretty_print_tracks(listened_tracks.loc[listened_liked_tracks_idx], indent=' ' * 4, enum=True)
    print()

    if len(listened_liked_tracks_idx) > 0:
        listened_liked_tracks_not_in_l2_idx = listened_liked_tracks_idx.difference(l2_queue_tracks.index,
                                                                                   sort=False)
        if len(listened_liked_tracks_not_in_l2_idx) < len(listened_liked_tracks_idx):
            print('WARNING: %d of these tracks are already in %s' %
                  (len(listened_liked_tracks_idx) - len(listened_liked_tracks_not_in_l2_idx), l2_queue_name))
            choice = get_user_choice('Continue?')
            if choice != 'yes':
                return l1_queue_tracks, l2_queue_tracks, liked_tracks, None

        if len(listened_liked_tracks_not_in_l2_idx) > 0:
            choice = get_user_choice('Add %d tracks to %s?' % (len(listened_liked_tracks_not_in_l2_idx), l2_queue_name))
            if choice == 'yes':
                spotify.add_tracks_to_playlist(l2_queue_id, listened_liked_tracks_not_in_l2_idx,
                                                   # avoid duplicate check since we've already done it
                                                   check_for_duplicates=False)

                l2_queue_tracks = pd.concat([l2_queue_tracks, l1_queue_tracks.loc[listened_liked_tracks_not_in_l2_idx]])
                print('%s now has %d tracks' % (l2_queue_name, len(l2_queue_tracks)))

    choice = get_user_choice('Add %d listened tracks to queue history?' % len(listened_tracks))
    if choice == 'yes':
        add_to_queue_history(listened_tracks)

    choice = get_user_choice('Remove %d listened tracks from queue?' % len(listened_tracks))
    if choice == 'yes':
        remove_from_queue(listened_tracks)

    choice = get_user_choice('Remove %d listened tracks from %s?' % (len(listened_tracks), l1_queue_name))
    if choice == 'yes':
        spotify.remove_tracks_from_playlist(l1_queue_id, listened_tracks.index)
        l1_queue_tracks = l1_queue_tracks.loc[l1_queue_tracks.index.difference(listened_tracks.index, sort=False)]
        print('%s now has %d tracks' % (l1_queue_name, len(l1_queue_tracks)))

    return l1_queue_tracks, l2_queue_tracks, liked_tracks


def replenish_l1_queue(
        l1_queue_name=DEFAULT_L1_QUEUE,
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
        choice = get_user_choice('Add up to %d new tracks to %s?' % (tracks_to_add, l1_queue_name))
        if choice == 'yes':
            queue_tracks = docs['queue'].read()
            print('Queue: %d tracks' % len(queue_tracks))

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

    return l1_queue_tracks

def sanity_check_l1_queue(
        l1_queue_name=DEFAULT_L1_QUEUE,
        l1_queue_id=None,
        l1_queue_tracks=None,
        liked_tracks=None):

    if l1_queue_id is None:
        l1_queue_id = spotify.get_playlist_id(l1_queue_name)

    if l1_queue_tracks is None:
        l1_queue_tracks = spotify.get_playlist_tracks(l1_queue_id)
        print('%s: %d tracks' % (l1_queue_name, len(l1_queue_tracks)))

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

            print('%s now has %d tracks' % (l1_queue_name, len(l1_queue_tracks)))

    # Make sure items in the L1 queue are unique
    dup_pos = dataframe_duplicate_index_labels(l1_queue_tracks)
    if len(dup_pos) > 0:
        print('WARNING: %s has %d duplicate tracks!' % (l1_queue_name, len(dup_pos)))
        pretty_print_tracks(l1_queue_tracks.iloc[dup_pos])

        choice = get_user_choice('Remove? (y/n)')
        if choice == 'yes':
            spotify.remove_tracks_from_playlist(l1_queue_id, l1_queue_tracks.index[dup_pos])

            l1_queue_tracks = dataframe_drop_rows_at_positions(l1_queue_tracks, dup_pos)

            print('%s now has %d tracks' % (l1_queue_name, len(l1_queue_tracks)))

    # Make sure all items in the L1 queue are not liked
    if liked_tracks is None:
        liked_tracks = spotify.get_liked_tracks()
        print('Spotify Liked Tracks: %d tracks' % len(liked_tracks))

    l1_queue_liked_tracks_idx = l1_queue_tracks.index.intersection(liked_tracks.index, sort=False)

    if len(l1_queue_liked_tracks_idx) > 0:
        print('WARNING: %d %s tracks are already liked' % (len(l1_queue_liked_tracks_idx), l1_queue_name))
        pretty_print_tracks(l1_queue_tracks.loc[l1_queue_liked_tracks_idx], indent=' '*4, enum=True)
        print()

        choice = get_user_choice('Unlike?')
        if choice == 'yes':
            spotify.remove_liked_tracks(l1_queue_liked_tracks_idx)

    liked_tracks = liked_tracks.loc[liked_tracks.index.difference(l1_queue_liked_tracks_idx, sort=False)]

    return l1_queue_tracks, liked_tracks


def add_to_l2_queue(
        tracks=[],
        l2_queue_name=DEFAULT_L2_QUEUE,
        l2_queue_id=None,
        l2_queue_tracks=None):

    tracks = dataframe_ensure_unique_index(tracks)
    print('Adding %d tracks to %s' % (len(tracks), l2_queue_name))

    if len(tracks) == 0:
        return

    if l2_queue_id is None:
        l2_queue_id = spotify.get_playlist_id(l2_queue_name)

    if l2_queue_tracks is None:
        l2_queue_tracks = spotify.get_playlist_tracks(l2_queue_id)
        print('%s: %d tracks' % (l2_queue_name, len(l2_queue_tracks)))

    new_tracks_idx = tracks.index.difference(l2_queue_tracks.index, sort=False)

    print('%d tracks are already in %s; adding remaining %d tracks' % (
        len(tracks) - len(new_tracks_idx),
        l2_queue_name,
        len(new_tracks_idx)
    ))
    new_tracks = tracks.loc[new_tracks_idx]
    pretty_print_tracks(new_tracks)

    spotify.add_tracks_to_playlist(l2_queue_id, new_tracks_idx, check_for_duplicates=False)

    l2_queue_tracks = pd.concat([l2_queue_tracks, new_tracks])

    choice = get_user_choice('Add tracks to queue history?')
    if choice == 'yes':
        add_to_queue_history(new_tracks)

    choice = get_user_choice('Remove tracks from queue?')
    if choice == 'yes':
        remove_from_queue(new_tracks, warn_for_missing=False)

    return l2_queue_tracks


def add_shazam_to_l2_queue(
        shazam_name = 'My Shazam Tracks',
        shazam_id=None,
        shazam_tracks=None,
        l2_queue_name=DEFAULT_L2_QUEUE,
        l2_queue_id=None,
        l2_queue_tracks=None):

    if shazam_id is None:
        shazam_id = spotify.get_playlist_id(shazam_name)

    if shazam_tracks is None:
        shazam_tracks = spotify.get_playlist_tracks(shazam_id)

    shazam_tracks = dataframe_ensure_unique_index(shazam_tracks)

    print("'%s' playlist: %d tracks" % (shazam_name, len(shazam_tracks)))
    pretty_print_tracks(shazam_tracks, indent=' '*4, enum=True)

    if len(shazam_tracks) == 0:
        return l2_queue_tracks

    choice = get_user_choice("Add to %s?" % l2_queue_name)
    if choice != 'yes':
        return l2_queue_tracks

    l2_queue_tracks = add_to_l2_queue(
        shazam_tracks,
        l2_queue_name=l2_queue_name,
        l2_queue_id=l2_queue_id,
        l2_queue_tracks=l2_queue_tracks
    )

    choice = get_user_choice("Remove tracks from '%s'?" % shazam_name)
    if choice == 'yes':
        spotify.remove_tracks_from_playlist(shazam_id, shazam_tracks.index)

    return l2_queue_tracks


def sanity_check_l2_queue(
        l2_queue_name=DEFAULT_L2_QUEUE,
        l2_queue_id=None,
        l2_queue_tracks=None,
        liked_tracks=None
):

    if l2_queue_tracks is None:
        if l2_queue_id is None:
            l2_queue_id = spotify.get_playlist_id(l2_queue_name)

        l2_queue_tracks = spotify.get_playlist_tracks(l2_queue_id)
        print('%s: %d tracks' % (l2_queue_name, len(l2_queue_tracks)))

    # Make sure all items in the L2 queue are already liked
    if liked_tracks is None:
        liked_tracks = spotify.get_liked_tracks()
        print('Spotify Liked Tracks: %d tracks' % len(liked_tracks))

    l2_queue_unliked_tracks_idx = l2_queue_tracks.index.difference(liked_tracks.index, sort=False)

    if len(l2_queue_unliked_tracks_idx) > 0:
        print('%d %s tracks are not in Liked Tracks' % (len(l2_queue_unliked_tracks_idx), l2_queue_name))
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

        print('%d %s tracks are not in queue history' % (len(l2_queue_not_in_queue_history_idx), l2_queue_name))
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

        print('%d %s tracks are in the queue' % (len(l2_queue_in_queue_idx), l2_queue_name))
        pretty_print_tracks(l2_queue_in_queue)
        choice = get_user_choice('Remove?')

        if choice == 'yes':
            queue = queue.loc[queue.index.difference(l2_queue_tracks.index, sort=False)]
            docs['queue'].write(queue)

    return l2_queue_tracks, liked_tracks

def queue_maintenance(
        l1_queue_name = DEFAULT_L1_QUEUE,
        l2_queue_name = DEFAULT_L2_QUEUE,
        shazam_name = 'My Shazam Tracks',
        last_track = None,
        l1_queue_target_size = 200
):

    # Sanity check! Queue and queue history must be disjoint
    sanity_check_disk_queues()

    l1_queue_id = spotify.get_playlist_id(l1_queue_name)
    l2_queue_id = spotify.get_playlist_id(l2_queue_name)

    l1_queue_tracks, l2_queue_tracks, liked_tracks = move_l1_queue_listened_tracks_to_l2(
        l1_queue_name=l1_queue_name,
        l2_queue_name=l2_queue_name,
        l1_queue_id=l1_queue_id,
        l2_queue_id=l2_queue_id,
        l1_queue_last_listened_track=last_track
    )

    l1_queue_tracks = replenish_l1_queue(
        l1_queue_name=l1_queue_name,
        l1_queue_id=l1_queue_id,
        l1_queue_tracks=l1_queue_tracks,
        target_size=l1_queue_target_size)

    l2_queue_tracks = add_shazam_to_l2_queue(
        shazam_name=shazam_name,
        l2_queue_name=l2_queue_name,
        l2_queue_id=l2_queue_id,
        l2_queue_tracks=l2_queue_tracks
       )

    l1_queue_tracks, liked_tracks = sanity_check_l1_queue(
        l1_queue_name=l1_queue_name,
        l1_queue_id=l1_queue_id,
        l1_queue_tracks=l1_queue_tracks,
        liked_tracks=liked_tracks)

    l2_queue_tracks, liked_tracks = sanity_check_l2_queue(
        l2_queue_name=l2_queue_name,
        l2_queue_id=l2_queue_id,
        l2_queue_tracks=l2_queue_tracks,
        liked_tracks=liked_tracks
    )

    return

def pretty_print_spotify_playlist(playlist_name):
    tracks = spotify.get_playlist_tracks(playlist_name)

    print("Spotify playlist '%s': %d tracks" % (playlist_name, len(tracks)))
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

