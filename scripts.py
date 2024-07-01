
import random

import pandas as pd
import numpy as np

from internal_utils import *
from utils import *



def _get_l1_queue_listened_tracks(ctx, l1_queue_tracks, l1_queue_last_listened_track):
    if l1_queue_last_listened_track is not None:
        if isinstance(l1_queue_last_listened_track, int):
            if l1_queue_last_listened_track > len(l1_queue_tracks):
                raise Exception('L1 queue last listened track is %d; L1 queue has only %d tracks' % (
                    l1_queue_last_listened_track,
                    len(l1_queue_tracks)
                ))

            return l1_queue_tracks.iloc[:l1_queue_last_listened_track]

        if isinstance(l1_queue_last_listened_track, str):
            l1_queue_last_listened_track_idx = None

            for i in range(len(l1_queue_tracks)):
                if l1_queue_tracks.iloc[i]['name'] == l1_queue_last_listened_track:
                    l1_queue_last_listened_track_idx = i
                    break

            if l1_queue_last_listened_track_idx is None:
                raise Exception("Track '%s' not found in L1 queue" % l1_queue_last_listened_track)

            return l1_queue_tracks.iloc[:l1_queue_last_listened_track_idx]

        raise Exception("Invalid type for l1_queue_last_listened_track: %s" % type(l1_queue_last_listened_track))

    print('WARNING: Attempting to determine the listened tracks in the L1 queue through the listening history; this may miss some tracks.')

    listened_tracks = ctx.spotify.get_recently_played_tracks()

    return l1_queue_tracks.loc[l1_queue_tracks.index.intersection(listened_tracks.index, sort=False)]


def manage_queues(
        ctx,
        l1_queue_name = 'L1 queue',
        l2_queue_name = 'L2 queue',
        l1_queue_last_listened_track = None,
        l1_queue_target_size = 100
):
    liked_tracks = None

    l1_queue_id = ctx.spotify.get_playlist_id(l1_queue_name)
    l2_queue_id = ctx.spotify.get_playlist_id(l2_queue_name)

    l1_queue_tracks = ctx.spotify.get_playlist_tracks(l1_queue_id)
    print('L1 queue: %d tracks' % len(l1_queue_tracks))

    l2_queue_tracks = ctx.spotify.get_playlist_tracks(l2_queue_id)
    print('L2 queue: %d tracks' % len(l2_queue_tracks))

    queue_history_tracks = ctx.docs['queue_history'].read()
    print('Queue history: %d tracks' % len(queue_history_tracks))

    queue_tracks = ctx.docs['queue'].read()
    print('Queue: %d tracks' % len(queue_tracks))

    # Sanity check! Queue and queue history must be disjoint
    queue_tracks_in_queue_history_idx = queue_tracks.index.intersection(queue_history_tracks.index, sort=False)
    if len(queue_tracks_in_queue_history_idx) > 0:
        print('WARNING: %d queue tracks are in queue history.')
        choice = get_user_choice('Remove?')

        if choice == 'yes':
            queue_tracks = queue_tracks.loc[queue_tracks.index.difference(queue_history_tracks.index, sort=False)]

            ctx.docs['queue'].write(queue_tracks)

            print('Queue now has %d tracks' % len(queue_tracks))

    if len(l1_queue_tracks) > 0:
        # get the listened tracks from L1
        listened_tracks = _get_l1_queue_listened_tracks(ctx, l1_queue_tracks, l1_queue_last_listened_track)
        print('L1 queue: %d listened tracks' % len(listened_tracks))
        pretty_print_tracks(listened_tracks, indent=' '*4, enum=True)
        print()

        if len(listened_tracks) > 0:
            # find how many of the listened tracks are liked
            if liked_tracks is None:
                liked_tracks = ctx.spotify.get_liked_tracks()

            listened_liked_tracks_idx = listened_tracks.index.intersection(liked_tracks.index, sort=False)
            print('L1 queue: %d of the %d listened tracks are liked' % (
                len(listened_liked_tracks_idx),
                len(listened_tracks)
            ))
            pretty_print_tracks(listened_tracks[listened_liked_tracks_idx], indent=' '*4, enum=True)
            print()

            if len(listened_liked_tracks_idx) > 0:
                listened_liked_tracks_not_in_l2_idx = listened_liked_tracks_idx.difference(l2_queue_tracks.index, sort=False)
                if len(listened_liked_tracks_not_in_l2_idx) < len(listened_liked_tracks_idx):
                    print('WARNING: %d of these tracks are already in the L2 queue' %
                          (len(listened_liked_tracks_idx) - len(listened_liked_tracks_not_in_l2_idx)))
                    choice = get_user_choice('Continue?')
                    if choice != 'yes':
                        return

                print('Adding %d tracks to the L2 queue' % len(listened_liked_tracks_not_in_l2_idx))
                ctx.spotify.add_tracks_to_playlist(l2_queue_id, listened_liked_tracks_not_in_l2_idx,
                                                   # avoid duplicate check since we've already done it
                                                   check_for_duplicates=False)

                print('L2 queue now has %d tracks' % (len(l2_queue_tracks) + len(listened_liked_tracks_not_in_l2_idx)))

            print('Adding the listened tracks to the queue history')

            listened_tracks_not_in_qh_idx = listened_tracks.index.difference(queue_history_tracks.index, sort=False)

            if len(listened_tracks_not_in_qh_idx) < len(listened_tracks):
                print('WARNING: %d listened tracks were already in queue history')
                choice = get_user_choice('Continue?')
                if choice != 'yes':
                    return

            queue_history_tracks = pd.concat([
                queue_history_tracks,
                listened_tracks.loc[listened_tracks_not_in_qh_idx]
            ])
            ctx['queue_history'].write(queue_history_tracks)
            print('Queue history now has %d tracks' % len(queue_history_tracks))
            print()

            print('Removing the listened tracks from the queue')

            remaining_queue_tracks_idx = queue_tracks.index.difference(listened_tracks.index, sort=False)

            if len(remaining_queue_tracks_idx) + len(listened_tracks) != len(queue_tracks):
                print('WARNING: %d listened tracks were already removed from the queue' %
                      (len(remaining_queue_tracks_idx) + len(listened_tracks) - len(queue_tracks)))
                choice = get_user_choice('Continue?')
                if choice != 'yes':
                    return

            queue_tracks = queue_tracks.loc[remaining_queue_tracks_idx]
            ctx['queue'].write(queue_tracks)
            print('Queue now has %d tracks' % len(queue_tracks))
            print()

            print('Removing the listened tracks from the L1 queue')

            ctx.spotify.remove_tracks_from_playlist(l1_queue_id, listened_tracks.index)

            l1_queue_tracks = l1_queue_tracks.loc[l1_queue_tracks.index.difference(listened_tracks.index, sort=False)]
            print('L1 queue now has %d tracks' % len(l1_queue_tracks))

    if l1_queue_target_size is not None and len(l1_queue_tracks) < l1_queue_target_size:
        print('Replenishing the L1 queue')

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

        l1_queue_tracks = pd.concat(l1_queue_tracks, tracks_to_add)

        print('L1 queue now has %d tracks' % (len(l1_queue_tracks) + len(tracks_to_add)))

    # Make sure all items in the L1 queue are not liked
    if liked_tracks is None:
        liked_tracks = ctx.spotify.get_liked_tracks()

    l1_queue_liked_tracks_idx = l1_queue_tracks.index.intersection(liked_tracks.index, sort=False)

    if len(l1_queue_liked_tracks_idx) > 0:
        print('WARNING: %d L1 queue tracks are already liked' % len(l1_queue_liked_tracks_idx))
        pretty_print_tracks(l1_queue_tracks.loc[l1_queue_liked_tracks_idx], indent=' '*4, enum=True)
        print()

        choice = get_user_choice('Unlike?')
        if choice == 'yes':
            ctx.spotify.remove_liked_tracks(l1_queue_liked_tracks_idx)

    return


def add_to_queue_history(ctx, new_items):
    queue_history = ctx.docs['queue_history']

    history = queue_history.read()

    if history.index.name != 'id':
        raise Exception('queue_history not indexed by ID')

    for column in history.columns:
        if column not in new_items.columns:
            raise Exception("New items are missing column '%s'" % column)

    # this takes care of extra columns, columns in different order etc.
    new_items = new_items[history.columns]

    if new_items.index.name == 'id':
        new_history_ids = new_items.index
    else:
        new_history_ids = pd.Index(new_items.id)

    unique_new_ids = new_history_ids.difference(history.index, sort=False)

    print('History has %d items' % len(history))
    print('Received %d new items, of which %d already exist; adding %d new items' % (
        len(new_items),
        len(new_items) - len(unique_new_ids),
          len(unique_new_ids)))

    new_history = pd.concat([history, new_items.loc[unique_new_ids]])

    queue_history.write(new_history)

    return
