
from utils import *


def add_to_history(ctx, new_items):
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

    existing_new_ids = new_history_ids.intersection(history.index)
    unique_new_ids = new_history_ids.difference(history.index)

    print('History has %d items' % len(history))
    print('Received %d new items, of which %d already exist; adding %d new items' % (
        len(new_items),
        len(existing_new_ids),
          len(unique_new_ids)))

    new_history = pd.concat([history, new_items.loc[unique_new_ids]])

    queue_history.write(new_history)

    return


def add_to_history_one_time(ctx):
    consider = ctx.spotify.get_playlist_tracks('consider')
    print('Playlist consider: %d items' % len(consider))

    backup_consider = ctx.spotify.get_playlist_tracks('backup - consider')
    print('Playlist backup - consider: %d items' % len(backup_consider))

    listened_index = backup_consider.index.difference(consider.index)

    print('Listened items: %d' % len(listened_index))

    consider_listened = backup_consider.loc[listened_index]

    assert len(consider_listened) == len(listened_index)

    add_to_history(ctx, consider_listened)

    return


def add_to_l2_queue_one_time(ctx):
    consider = ctx.spotify.get_playlist_tracks('consider', stop=137)

    print('Consider tracks: %d' % len(consider))

    print('Adding to history...')
    add_to_history(ctx, consider)

    liked = ctx.spotify.get_liked_tracks(stop=33)
    print('Liked tracks: %d' % len(liked))

    liked_consider_index = consider.index.intersection(liked.index)
    print('Liked consider: %d' % len(liked_consider_index))

    print('Adding to L2 queue...')

    ctx.spotify.add_tracks_to_playlist('L2 queue', liked_consider_index)

    return






