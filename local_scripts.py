
import pandas as pd
import numpy as np

from internal_utils import *
from utils import *

context = None

def rekordbox_sanity_checks(ctx=None):
    global context

    if ctx is None:
        ctx = context

    top_level_playlist_names = ['Main Library', 'back catalog', 'non-DJ']

    collection = ctx.rekordbox.get_collection()
    print('Rekordbox collection: %d tracks' % len(collection))

    errors = 0

    # check that all top-level playlists exist
    top_level_playlists = []
    for name in top_level_playlist_names:
        try:
            playlist = ctx.rekordbox.get_playlist_track_ids(name)
            top_level_playlists.append(playlist)
        except KeyError:
            sys.stderr.write("Top-level rekordbox playlist '%s' does not exist\n" % playlist)
            top_level_playlists.append(None)
            errors += 1

    print('Top-level playlists:')
    for i in range(len(top_level_playlist_names)):
        if top_level_playlists[i] is None:
            continue

        print('    %s: %d tracks' % (top_level_playlist_names[i], len(top_level_playlists[i])))

    # check that top-level playlists do not overlap
    for i in range(len(top_level_playlist_names)):
        if top_level_playlists[i] is None:
            continue

        for j in range(i+1, len(top_level_playlist_names)):
            if top_level_playlists[j] is None:
                continue

            intersection = top_level_playlists[i].intersection(top_level_playlists[j], sort=False)

            for track_id in intersection:
                sys.stderr.write("Track appears in two top-level playlists, %s and %s: %s\n" % (
                    top_level_playlist_names[i],
                    top_level_playlist_names[j],
                    format_track(collection.loc[track_id])
                ))
                errors += 1

    # check that every track is in a top-level playlist
    track_ids_without_top_level = collection.index

    for top_level_playlist in top_level_playlists:
        if top_level_playlist is None:
            continue
        track_ids_without_top_level = track_ids_without_top_level.difference(top_level_playlist, sort=False)

    for track_id in track_ids_without_top_level:
        sys.stderr.write("Track is not in any top-level playlist: %s\n" % format_track(collection.loc[track_id]))
        errors += 1

    if errors > 0:
        sys.stderr.write('Rekordbox state is inconsistent; further operations are not safe.')
        return False

    return True

def djlib_sanity_checks(ctx=None):
    global context
    if ctx is None:
        ctx = context

    djlib = ctx.docs['djlib']
    djlib_tracks = djlib.read()

    print('djlib excel sheet: %d tracks' % len(djlib_tracks))

    rekordbox_tracks = ctx.rekordbox.get_collection()
    main_library_tracks = ctx.rekordbox.get_playlist_tracks('Main Library')

    errors = 0

    if djlib_tracks.index.dtype != pd.Int64Dtype():
        # this will mess everything up
        errors += 1
        sys.stderr.write('Bad type for djlib index: %s - should be %s. This will mess everything up.\n' % (
            djlib_tracks.index.dtype,
            pd.Int64Dtype()
        ))
    else:
        no_track_ids_idx = djlib_tracks.index.isna()

        no_track_ids = djlib_tracks[no_track_ids_idx]

        for i in range(len(no_track_ids)):
            sys.stderr.write('Track has no Rekordbox ID: %s\n' % format_track(no_track_ids.iloc[i]))
            errors += 1

        with_track_ids = djlib_tracks.loc[~no_track_ids_idx]

        not_found_in_rekordbox_idx = with_track_ids.index.difference(rekordbox_tracks.index)
        not_found_in_rekordbox = djlib_tracks.loc[not_found_in_rekordbox_idx]

        for i in range(len(not_found_in_rekordbox)):
            sys.stderr.write('Track not found in Rekordbox: %s\n' % format_track(not_found_in_rekordbox.iloc[i]))
            errors += 1

    if errors > 0:
        sys.stderr.write('djlib state is inconsistent; further operations are not safe.\n')
        return False

    return True

# def djlib_maintenance(ctx=None):
#     global context
#     if ctx is None:
#         ctx = context
