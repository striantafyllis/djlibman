
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

def djlib_maintenance(ctx=None):
    global context
    if ctx is None:
        ctx = context

    djlib_tracks_changed = False

    djlib = ctx.docs['djlib']
    new_tracks_sheet = ctx.docs['new_tracks_scratch']

    djlib_tracks = djlib.read()
    rekordbox_main_library_tracks = ctx.rekordbox.get_playlist_tracks('Main Library')

    if djlib_tracks.index.name != rekordbox_main_library_tracks.index.name:
        raise Exception('djlib and Rekordbox have different indices: %s vs. %s' % (
            djlib_tracks.index.name, rekordbox_main_library_tracks.index.name))

    # filter out local edits
    should_transfer = rekordbox_main_library_tracks.Title.apply(
        lambda title: not title.endswith('[Local Edit]')
    )

    missing_from_djlib_idx = (rekordbox_main_library_tracks
        .index[should_transfer]
        .difference(djlib_tracks.index, sort=False))

    djlib_auto_columns = djlib_tracks.columns.intersection(
        rekordbox_main_library_tracks.columns, sort=False)
    djlib_user_columns = djlib_tracks.columns.difference(djlib_auto_columns, sort=False)

    if len(missing_from_djlib_idx) == 0:
        print('All Rekordbox Main Library tracks are in djlib')
    else:
        assert djlib_tracks.index.name in djlib_auto_columns

        new_rekordbox_tracks = rekordbox_main_library_tracks[djlib_auto_columns].loc[missing_from_djlib_idx]

        print('%d Rekordbox Main Library tracks are missing from djlib' % len(new_rekordbox_tracks))
        pretty_print_tracks(new_rekordbox_tracks, indent=' '*4, enum=True)
        choice = get_user_choice('Add to djlib?')

        if choice == 'yes':
            new_djlib_tracks = pd.DataFrame(
                index=new_rekordbox_tracks.index,
                columns=djlib_tracks.columns)

            new_djlib_tracks[djlib_auto_columns] = new_rekordbox_tracks

            djlib_tracks = pd.concat([djlib_tracks, new_djlib_tracks])
            djlib_tracks_changed = True

            print('djlib now has %d tracks' % len(djlib_tracks))

    # see if there are any empty rows in djlib
    djlib_tracks_is_empty_row = None
    for column in djlib_user_columns:
        column_is_empty = pd.isna(djlib_tracks[column])
        if djlib_tracks_is_empty_row is None:
            djlib_tracks_is_empty_row = column_is_empty
        else:
            djlib_tracks_is_empty_row &= column_is_empty

    djlib_tracks_empty_rows = djlib_tracks.loc[djlib_tracks_is_empty_row]

    if len(djlib_tracks_empty_rows) == 0:
        print('No empty rows in djlib; skipping New Tracks sheet')
    else:
        print('djlib has %d empty rows; looking in New Tracks sheet' % len(djlib_tracks_empty_rows))
        pretty_print_tracks(djlib_tracks_empty_rows, indent=' '*4, enum=True)

        new_tracks = new_tracks_sheet.read()

        if len(new_tracks) == 0:
            print('Nothing in New Tracks sheet')
        else:
            new_tracks_unknown_cols = new_tracks.columns.difference(djlib_tracks.columns, sort=False)

            if len(new_tracks_unknown_cols) > 0:
                sys.stderr.write('New Tracks sheet has unknown columns %s; these will be ignored.\n' %
                                 new_tracks_unknown_cols.to_list())

            new_tracks_usable_cols = new_tracks.columns.intersection(djlib_tracks.columns, sort=False)

            if len(new_tracks_usable_cols) == 0:
                sys.stderr.write('New Tracks sheet has no usable columns; ignoring.\n')
            elif 'Title' not in new_tracks_usable_cols:
                sys.stderr.write('New Tracks does not have a Title column; ignoring.\n')
            else:
                new_tracks_usable_cols = new_tracks_usable_cols[new_tracks_usable_cols != 'Title']

                djlib_tracks_idx = []
                new_tracks_idx = []

                for i in range(len(new_tracks)):
                    new_track_title = new_tracks.Title.iloc[i]
                    djlib_tracks_loc = None
                    for j in range(len(djlib_tracks_empty_rows)):
                        djlib_title = djlib_tracks_empty_rows.Title.iloc[j]
                        if new_track_title.upper() in djlib_title.upper():
                            djlib_tracks_loc = j
                            break

                    if djlib_tracks_loc is None:
                        print("No hit for New Tracks title '%s'" % new_track_title)
                    else:
                        djlib_tracks_idx.append(djlib_tracks_empty_rows.index[djlib_tracks_loc])
                        new_tracks_idx.append(new_tracks.index[i])

                if len(new_tracks_idx) == 0:
                    print('No usable entries found in New Tracks')
                else:
                    usable_new_tracks = new_tracks.loc[new_tracks_idx]
                    print('Found %d usable entries in New Tracks:' % len(usable_new_tracks))
                    for title in usable_new_tracks.Title:
                        print('    %s' % title)
                    choice = get_user_choice('Write to djlib?')
                    if choice == 'yes':
                        # for col in new_tracks_usable_cols:
                        #     # the astype conversion is necessary for some columns that are usually empty in New Tracks - e.g. Notes
                        #     djlib_tracks.loc[djlib_tracks_idx, col] = usable_new_tracks[col].to_numpy()

                        # the to_numpy() call works around the differences in the indices and the types
                        djlib_tracks.loc[djlib_tracks_idx, new_tracks_usable_cols] =\
                            usable_new_tracks[new_tracks_usable_cols].to_numpy()
                        djlib_tracks_changed = True

    if djlib_tracks_changed:
        djlib.write(djlib_tracks)

    return






