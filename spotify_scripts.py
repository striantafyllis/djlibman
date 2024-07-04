
import random

import pandas as pd
import numpy as np

from internal_utils import *
from utils import *

context = None

def sanity_check_disk_queues(ctx=None, queue_tracks=None, queue_history_tracks=None):
    if ctx is None:
        ctx = context

    if queue_tracks is None:
        queue_tracks = ctx.docs['queue'].read()
        print('Queue: %d tracks' % len(queue_tracks))
    if queue_history_tracks is None:
        queue_history_tracks = ctx.docs['queue_history'].read()
        print('Queue history: %d tracks' % len(queue_history_tracks))

    # entries in the queue should be unique
    dup_pos = dataframe_duplicate_index_labels(queue_tracks)
    if len(dup_pos) > 0:
        print('WARNING: Queue has %d duplicate tracks!' % len(dup_pos))
        pretty_print_tracks(queue_tracks.iloc[dup_pos], indent=' '*4, enum=True)
        choice = get_user_choice('Remove? (y/n)')
        if choice == 'yes':
            queue_tracks = dataframe_drop_rows_at_positions(queue_tracks, dup_pos)
            ctx.docs['queue'].write(queue_tracks)

            print('Queue now has %d tracks' % len(queue_tracks))

    # entries in queue history should be unique
    dup_pos = dataframe_duplicate_index_labels(queue_history_tracks)
    if len(dup_pos) > 0:
        print('WARNING: Queue history has %d duplicate tracks!' % len(dup_pos))
        pretty_print_tracks(queue_history_tracks.iloc[dup_pos], indent=' ' * 4, enum=True)
        choice = get_user_choice('Remove? (y/n)')
        if choice == 'yes':
            queue_history_tracks = dataframe_drop_rows_at_positions(queue_history_tracks, dup_pos)
            ctx.docs['queue_history'].write(queue_history_tracks)

            print('Queue history now has %d tracks' % len(queue_history_tracks))

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

            ctx.docs['queue'].write(queue_tracks)

            print('Queue now has %d tracks' % len(queue_tracks))

    print()

    return


def add_to_queue_history(ctx=None, new_tracks=[]):
    if ctx is None:
        ctx = context

    queue_history = ctx.docs['queue_history']
    queue_history_tracks = queue_history.read()
    print('Queue history: %d tracks' % len(queue_history_tracks))

    if queue_history_tracks.index.name != 'id':
        raise Exception('queue_history not indexed by ID')

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


def remove_from_queue(ctx=None, listened_tracks=[], warn_for_missing=True):
    if ctx is None:
        ctx = context

    queue_tracks = ctx.docs['queue'].read()
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
    ctx.docs['queue'].write(queue_tracks)
    print('Queue now has %d tracks' % len(queue_tracks))

    return


def get_playlist_listened_tracks(
        ctx=None,
        playlist_name=None,
        playlist_id=None,
        playlist_tracks=None,
        last_listened_track=None):

    if ctx is None:
        ctx = context

    if playlist_tracks is None:
        if playlist_id is None:
            if playlist_name is None:
                raise Exception("At least one of playlist_tracks, playlist_id or playlist_name must be specified")

            playlist_id = ctx.spotify.get_playlist_id(playlist_name)
            playlist_tracks = ctx.spotify.get_playlist_tracks(playlist_id)

    if last_listened_track is not None:
        if isinstance(last_listened_track, int):
            if last_listened_track > len(playlist_tracks):
                raise Exception('Playlist last listened track is %d; playlist has only %d tracks' % (
                    last_listened_track,
                    len(playlist_tracks)
                ))

            return playlist_tracks.iloc[:last_listened_track]

        if isinstance(last_listened_track, str):
            last_listened_track_idx = None

            for i in range(len(playlist_tracks)):
                if playlist_tracks.iloc[i]['name'] == last_listened_track:
                    last_listened_track_idx = i
                    break

            if last_listened_track_idx is None:
                raise Exception("Track '%s' not found in playlist" % last_listened_track)

            return playlist_tracks.iloc[:(last_listened_track_idx+1)]

        raise Exception("Invalid type for last_listened_track: %s" % type(last_listened_track))

    print('WARNING: Attempting to determine listened tracks through the listening history; this may miss some tracks.')
    choice = get_user_choice('Continue?')
    if choice != 'yes':
        return pd.DataFrame([])

    listened_tracks = ctx.spotify.get_recently_played_tracks()

    return playlist_tracks.loc[playlist_tracks.index.intersection(listened_tracks.index, sort=False)]



def move_l1_queue_listened_tracks_to_l2(
        ctx=None,
        l1_queue_name='L1 queue',
        l2_queue_name='L2 queue',
        l1_queue_id=None,
        l2_queue_id=None,
        l1_queue_last_listened_track=None):

    if ctx is None:
        ctx = context

    if l1_queue_id is None:
        l1_queue_id = ctx.spotify.get_playlist_id(l1_queue_name)
    if l2_queue_id is None:
        l2_queue_id = ctx.spotify.get_playlist_id(l2_queue_name)

    l1_queue_tracks = ctx.spotify.get_playlist_tracks(l1_queue_id)
    print('L1 queue: %d tracks' % len(l1_queue_tracks))

    if l1_queue_tracks.empty:
        return l1_queue_tracks, None

    l2_queue_tracks = ctx.spotify.get_playlist_tracks(l2_queue_id)
    print('L2 queue: %d tracks' % len(l2_queue_tracks))

    listened_tracks = get_playlist_listened_tracks(
        ctx,
        playlist_tracks=l1_queue_tracks,
        last_listened_track=l1_queue_last_listened_track)

    print('L1 queue: %d listened tracks' % len(listened_tracks))
    pretty_print_tracks(listened_tracks, indent=' ' * 4, enum=True)
    print()
    choice = get_user_choice('Is this correct?')
    if choice != 'yes':
        return l1_queue_tracks, l2_queue_tracks, None

    if len(listened_tracks) == 0:
        return l1_queue_tracks, l2_queue_tracks, None

    # find how many of the listened tracks are liked
    liked_tracks = ctx.spotify.get_liked_tracks()
    print('Spotify Liked Tracks: %d tracks' % len(liked_tracks))

    listened_liked_tracks_idx = listened_tracks.index.intersection(liked_tracks.index, sort=False)
    print('L1 queue: %d of the %d listened tracks are liked' % (
        len(listened_liked_tracks_idx),
        len(listened_tracks)
    ))
    pretty_print_tracks(listened_tracks.loc[listened_liked_tracks_idx], indent=' ' * 4, enum=True)
    print()

    if len(listened_liked_tracks_idx) > 0:
        listened_liked_tracks_not_in_l2_idx = listened_liked_tracks_idx.difference(l2_queue_tracks.index,
                                                                                   sort=False)
        if len(listened_liked_tracks_not_in_l2_idx) < len(listened_liked_tracks_idx):
            print('WARNING: %d of these tracks are already in the L2 queue' %
                  (len(listened_liked_tracks_idx) - len(listened_liked_tracks_not_in_l2_idx)))
            choice = get_user_choice('Continue?')
            if choice != 'yes':
                return l1_queue_tracks, l2_queue_tracks, liked_tracks

        if len(listened_liked_tracks_not_in_l2_idx) > 0:
            choice = get_user_choice('Add %d tracks to the L2 queue?' % len(listened_liked_tracks_not_in_l2_idx))
            if choice == 'yes':
                ctx.spotify.add_tracks_to_playlist(l2_queue_id, listened_liked_tracks_not_in_l2_idx,
                                                   # avoid duplicate check since we've already done it
                                                   check_for_duplicates=False)

                l2_queue_tracks = pd.concat([l2_queue_tracks, l1_queue_tracks.loc[listened_liked_tracks_not_in_l2_idx]])
                print('L2 queue now has %d tracks' % (len(l2_queue_tracks)))

    choice = get_user_choice('Add %d listened tracks to queue history?' % len(listened_tracks))
    if choice == 'yes':
        add_to_queue_history(ctx, listened_tracks)

    choice = get_user_choice('Remove %d listened tracks from queue?' % len(listened_tracks))
    if choice == 'yes':
        remove_from_queue(ctx, listened_tracks)

    choice = get_user_choice('Remove %d listened tracks from L1 queue?' % len(listened_tracks))
    if choice == 'yes':
        ctx.spotify.remove_tracks_from_playlist(l1_queue_id, listened_tracks.index)
        l1_queue_tracks = l1_queue_tracks.loc[l1_queue_tracks.index.difference(listened_tracks.index, sort=False)]
        print('L1 queue now has %d tracks' % len(l1_queue_tracks))

    return l1_queue_tracks, l2_queue_tracks, liked_tracks


def replenish_l1_queue(
        ctx=None,
        l1_queue_name='L1 queue',
        l1_queue_id=None,
        l1_queue_tracks=None,
        l1_queue_target_size=100):

    if ctx is None:
        ctx = context

    if l1_queue_tracks is None:
        if l1_queue_id is None:
            l1_queue_id = ctx.spotify.get_playlist_id(l1_queue_name)

        l1_queue_tracks = ctx.spotify.get_playlist_tracks(l1_queue_id)

    if l1_queue_target_size is None or len(l1_queue_tracks) >= l1_queue_target_size:
        return l1_queue_tracks

    tracks_to_add = l1_queue_target_size - len(l1_queue_tracks)

    choice = get_user_choice('Add up to %d new tracks to the L1 queue?' % tracks_to_add)
    if choice != 'yes':
        return l1_queue_tracks

    queue_tracks = ctx.docs['queue'].read()
    print('Queue: %d tracks' % len(queue_tracks))

    queue_tracks_not_in_l1_queue_idx = queue_tracks.index.difference(l1_queue_tracks.index, sort=False)

    num_tracks_to_add = min(l1_queue_target_size - len(l1_queue_tracks), len(queue_tracks_not_in_l1_queue_idx))

    if num_tracks_to_add < len(queue_tracks_not_in_l1_queue_idx):
        tracks_to_add_idx = random.choices(queue_tracks_not_in_l1_queue_idx, k=num_tracks_to_add)
        tracks_to_add = queue_tracks.loc[tracks_to_add_idx]
    else:
        tracks_to_add = queue_tracks.loc[queue_tracks_not_in_l1_queue_idx]

    print('Adding %d tracks to the L1 queue' % num_tracks_to_add)
    pretty_print_tracks(tracks_to_add, indent=' '*4, enum=True)

    ctx.spotify.add_tracks_to_playlist(l1_queue_id, tracks_to_add.index,
                                       # skip the duplicate check since we've already done it
                                       check_for_duplicates=False)

    l1_queue_tracks = pd.concat([l1_queue_tracks, tracks_to_add])

    print('L1 queue now has %d tracks' % (len(l1_queue_tracks)))

    return l1_queue_tracks

def sanity_check_l1_queue(
        ctx=None,
        l1_queue_name='L1 queue',
        l1_queue_id=None,
        l1_queue_tracks=None,
        liked_tracks=None):
    if ctx is None:
        ctx = context
    if l1_queue_id is None:
        l1_queue_id = ctx.spotify.get_playlist_id(l1_queue_name)

    if l1_queue_tracks is None:
        l1_queue_tracks = ctx.spotify.get_playlist_tracks(l1_queue_id)
        print('L1 queue: %d tracks' % len(l1_queue_tracks))

    # Make sure all items in the L1 queue are also in the disk queue
    queue_tracks = ctx.docs['queue'].read()

    l1_queue_tracks_not_in_queue_idx = l1_queue_tracks.index.difference(queue_tracks.index, sort=False)
    if len(l1_queue_tracks_not_in_queue_idx) > 0:
        print('WARNING: %d tracks are in L1 queue but not in disk queue' % len(l1_queue_tracks_not_in_queue_idx))
        pretty_print_tracks(l1_queue_tracks.loc[l1_queue_tracks_not_in_queue_idx])

        choice = get_user_choice('Remove?')
        if choice == 'yes':
            ctx.spotify.remove_tracks_from_playlist(l1_queue_id, l1_queue_tracks_not_in_queue_idx)

            l1_queue_tracks = l1_queue_tracks.loc[l1_queue_tracks.index.difference(l1_queue_tracks_not_in_queue_idx, sort=False)]

    # Make sure all items in the L1 queue are not in queue history
    queue_history_tracks = ctx.docs['queue_history'].read()

    l1_queue_tracks_in_qh_idx = l1_queue_tracks.index.intersection(queue_history_tracks.index, sort=False)
    if len(l1_queue_tracks_in_qh_idx) > 0:
        print('WARNING: %d tracks from L1 queue are in queue history' % len(l1_queue_tracks_in_qh_idx))
        pretty_print_tracks(l1_queue_tracks.loc[l1_queue_tracks_in_qh_idx])

        choice = get_user_choice('Remove from L1 queue?')
        if choice == 'yes':
            ctx.spotify.remove_tracks_from_playlist(l1_queue_id, l1_queue_tracks_in_qh_idx)

            l1_queue_tracks = l1_queue_tracks.loc[l1_queue_tracks.index.difference(l1_queue_tracks_in_qh_idx, sort=False)]
            print("L1 queue now has %d tracks" % len(l1_queue_tracks))

    # Make sure all items in the L1 queue are not liked
    if liked_tracks is None:
        liked_tracks = ctx.spotify.get_liked_tracks()
        print('Spotify Liked Tracks: %d tracks' % len(liked_tracks))

    l1_queue_liked_tracks_idx = l1_queue_tracks.index.intersection(liked_tracks.index, sort=False)

    if len(l1_queue_liked_tracks_idx) > 0:
        print('WARNING: %d L1 queue tracks are already liked' % len(l1_queue_liked_tracks_idx))
        pretty_print_tracks(l1_queue_tracks.loc[l1_queue_liked_tracks_idx], indent=' '*4, enum=True)
        print()

        choice = get_user_choice('Unlike?')
        if choice == 'yes':
            ctx.spotify.remove_liked_tracks(l1_queue_liked_tracks_idx)

    liked_tracks = liked_tracks.loc[liked_tracks.index.difference(l1_queue_liked_tracks_idx, sort=False)]

    return l1_queue_tracks, liked_tracks


def add_to_l2_queue(
        ctx=None,
        tracks=[],
        l2_queue_name='L2 queue',
        l2_queue_id=None,
        l2_queue_tracks=None):
    if ctx is None:
        ctx = context

    tracks = dataframe_ensure_unique_index(tracks)
    print('Adding %d tracks to L2 queue' % len(tracks))

    if len(tracks) == 0:
        return

    if l2_queue_id is None:
        l2_queue_id = ctx.spotify.get_playlist_id(l2_queue_name)

    if l2_queue_tracks is None:
        l2_queue_tracks = ctx.spotify.get_playlist_tracks(l2_queue_id)
        print('L2 queue: %d tracks' % len(l2_queue_tracks))

    new_tracks_idx = tracks.index.difference(l2_queue_tracks.index, sort=False)

    print('%d tracks are already in L2 queue; adding remaining %d tracks' % (
        len(tracks) - len(new_tracks_idx),
        len(new_tracks_idx)
    ))
    new_tracks = tracks.loc[new_tracks_idx]
    pretty_print_tracks(new_tracks)

    ctx.spotify.add_tracks_to_playlist(l2_queue_id, new_tracks_idx, check_for_duplicates=False)

    l2_queue_tracks = pd.concat([l2_queue_tracks, new_tracks])

    choice = get_user_choice('Add tracks to queue history?')
    if choice == 'yes':
        add_to_queue_history(ctx, new_tracks)

    choice = get_user_choice('Remove tracks from queue?')
    if choice == 'yes':
        remove_from_queue(ctx, new_tracks, warn_for_missing=False)

    return l2_queue_tracks


def add_shazam_to_l2_queue(
        ctx=None,
        shazam_name = 'My Shazam Tracks',
        shazam_id=None,
        shazam_tracks=None,
        l2_queue_name='L2 queue',
        l2_queue_id=None,
        l2_queue_tracks=None):
    if ctx is None:
        ctx = context

    if shazam_id is None:
        shazam_id = ctx.spotify.get_playlist_id(shazam_name)

    if shazam_tracks is None:
        shazam_tracks = ctx.spotify.get_playlist_tracks(shazam_id)

    print("'%s' playlist: %d tracks" % (shazam_name, len(shazam_tracks)))

    if len(shazam_tracks) == 0:
        return l2_queue_tracks

    choice = get_user_choice("Add to L2 queue?")
    if choice != 'yes':
        return l2_queue_tracks

    l2_queue_tracks = add_to_l2_queue(
        ctx,
        shazam_tracks,
        l2_queue_name=l2_queue_name,
        l2_queue_id=l2_queue_id,
        l2_queue_tracks=l2_queue_tracks
    )

    choice = get_user_choice("Remove tracks from '%s'?" % shazam_name)
    if choice == 'yes':
        ctx.spotify.remove_tracks_from_playlist(shazam_id, shazam_tracks.index)

    return l2_queue_tracks


def sanity_check_l2_queue(
        ctx=None,
        l2_queue_name='L2 queue',
        l2_queue_id=None,
        l2_queue_tracks=None,
        liked_tracks=None
):
    if ctx is None:
        ctx = context

    if l2_queue_tracks is None:
        if l2_queue_id is None:
            l2_queue_id = ctx.spotify.get_playlist_id(l2_queue_name)

        l2_queue_tracks = ctx.spotify.get_playlist_tracks(l2_queue_id)
        print('L2 queue: %d tracks' % len(l2_queue_tracks))

    # Make sure all items in the L2 queue are already liked
    if liked_tracks is None:
        liked_tracks = ctx.spotify.get_liked_tracks()
        print('Spotify Liked Tracks: %d tracks' % len(liked_tracks))

    l2_queue_unliked_tracks_idx = l2_queue_tracks.index.difference(liked_tracks.index, sort=False)

    if len(l2_queue_unliked_tracks_idx) > 0:
        print('%d L2 queue tracks are not in Liked Tracks' % len(l2_queue_unliked_tracks_idx))
        l2_queue_unliked_tracks = l2_queue_tracks.loc[l2_queue_unliked_tracks_idx]
        pretty_print_tracks(l2_queue_unliked_tracks)
        choice = get_user_choice('Add?')
        if choice == 'yes':
            ctx.spotify.add_liked_tracks(l2_queue_unliked_tracks_idx)

            liked_tracks = pd.concat([liked_tracks, l2_queue_unliked_tracks])

    # Make sure all items in the L2 queue are in queue_history
    queue_history = ctx.docs['queue_history'].read()

    l2_queue_not_in_queue_history_idx = l2_queue_tracks.index.difference(queue_history.index)

    if len(l2_queue_not_in_queue_history_idx) > 0:
        l2_queue_not_in_queue_history = l2_queue_tracks.loc[l2_queue_not_in_queue_history_idx]

        print('%d L2 queue tracks are not in queue history' % len(l2_queue_not_in_queue_history_idx))
        pretty_print_tracks(l2_queue_not_in_queue_history)
        choice = get_user_choice('Add?')

        if choice == 'yes':
            queue_history = pd.concat([queue_history, l2_queue_not_in_queue_history])
            ctx.docs['queue_history'].write(queue_history)

    # Make sure all items in the L2 queue are not in queue
    queue = ctx.docs['queue'].read()

    l2_queue_in_queue_idx = l2_queue_tracks.index.intersection(queue.index)

    if len(l2_queue_in_queue_idx) > 0:
        l2_queue_in_queue = l2_queue_tracks.loc[l2_queue_in_queue_idx]

        print('%d L2 queue tracks are in the queue' % len(l2_queue_in_queue_idx))
        pretty_print_tracks(l2_queue_in_queue)
        choice = get_user_choice('Remove?')

        if choice == 'yes':
            queue = queue.loc[queue.index.difference(l2_queue_tracks.index, sort=False)]
            ctx.docs['queue'].write(queue)

    return l2_queue_tracks, liked_tracks

def manage_spotify_queues(
        ctx=None,
        l1_queue_name = 'L1 queue',
        l2_queue_name = 'L2 queue',
        shazam_name = 'My Shazam Tracks',
        l1_queue_last_listened_track = None,
        # alias for l1_queue_last_listened_track to save typing
        last_track = None,
        l1_queue_target_size = 100
):
    if ctx is None:
        ctx = context

    if l1_queue_last_listened_track is None:
        l1_queue_last_listened_track = last_track
    elif last_track is not None:
        raise Exception('Both last_track and l1_queue_last_listened_track are set')

    # Sanity check! Queue and queue history must be disjoint
    sanity_check_disk_queues(ctx)

    l1_queue_id = ctx.spotify.get_playlist_id(l1_queue_name)
    l2_queue_id = ctx.spotify.get_playlist_id(l2_queue_name)

    l1_queue_tracks, l2_queue_tracks, liked_tracks = move_l1_queue_listened_tracks_to_l2(
        ctx,
        l1_queue_name=l1_queue_name,
        l2_queue_name=l2_queue_name,
        l1_queue_id=l1_queue_id,
        l2_queue_id=l2_queue_id,
        l1_queue_last_listened_track=l1_queue_last_listened_track
    )

    l1_queue_tracks = replenish_l1_queue(
        ctx,
        l1_queue_name=l1_queue_name,
        l1_queue_id=l1_queue_id,
        l1_queue_tracks=l1_queue_tracks,
        l1_queue_target_size=l1_queue_target_size)

    l2_queue_tracks = add_shazam_to_l2_queue(
        ctx,
        shazam_name=shazam_name,
        l2_queue_name=l2_queue_name,
        l2_queue_id=l2_queue_id,
        l2_queue_tracks=l2_queue_tracks
       )

    l1_queue_tracks, liked_tracks = sanity_check_l1_queue(
        ctx,
        l1_queue_name=l1_queue_name,
        l1_queue_id=l1_queue_id,
        l1_queue_tracks=l1_queue_tracks,
        liked_tracks=liked_tracks)

    l2_queue_tracks, liked_tracks = sanity_check_l2_queue(
        ctx,
        l2_queue_name=l2_queue_name,
        l2_queue_id=l2_queue_id,
        l2_queue_tracks=l2_queue_tracks,
        liked_tracks=liked_tracks
    )

    return

