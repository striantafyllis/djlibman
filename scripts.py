
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
