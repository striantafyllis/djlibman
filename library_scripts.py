
import pandas as pd
import numpy as np

from djlib_config import *
from general_utils import *
# from playlist_scripts import *
from containers import *

def rekordbox_sanity_checks():
    top_level_playlist_names = ['Main Library', 'back catalog', 'non-DJ']

    collection = RekordboxCollection()
    print('Rekordbox collection: %d tracks' % len(collection))

    errors = 0

    # check that all top-level playlists exist
    print('Top-level playlists:')
    top_level_playlists = []
    for name in top_level_playlist_names:
        playlist = RekordboxPlaylist(name)
        top_level_playlists.append(playlist)
        if not playlist.exists():
            sys.stderr.write("Top-level rekordbox playlist '%s' does not exist\n" % playlist)
            errors += 1
        else:
            print(f'    {playlist.get_name()}: {len(playlist)} tracks')


    # check that top-level playlists do not overlap
    for i in range(len(top_level_playlist_names)):
        if not top_level_playlists[i].exists():
            continue

        for j in range(i+1, len(top_level_playlist_names)):
            if not top_level_playlists[j].exists():
                continue

            intersection = top_level_playlists[i].get_intersection(top_level_playlists[j])

            for track in intersection.values:
                sys.stderr.write(
                    f"Track appears in two top-level playlists, {top_level_playlist_names[i]} and "
                    f"{top_level_playlist_names[j]}: "
                    f"{format_track(track)}\n"
                )
                errors += 1

    # check that every track is in a top-level playlist
    track_ids_without_top_level = collection.get_df().index

    for top_level_playlist in top_level_playlists:
        if not top_level_playlist.exists():
            continue
        track_ids_without_top_level = track_ids_without_top_level.difference(top_level_playlist.get_df().index, sort=False)

    for track_id in track_ids_without_top_level:
        sys.stderr.write("Track is not in any top-level playlist: %s\n" % format_track(collection.get_df().loc[track_id]))
        errors += 1

    if errors > 0:
        sys.stderr.write('Rekordbox state is inconsistent; further operations are not safe.\n')
        return False

    return True

def djlib_sanity_checks():
    djlib = Doc('djlib')

    print('djlib excel sheet: %d tracks' % len(djlib))

    collection = RekordboxCollection()

    errors = 0

    djlib_index = djlib.get_df().index
    if djlib_index.name != 'rekordbox_id' or djlib_index.dtype != pd.Int64Dtype():
        # this will mess everything up
        errors += 1
        sys.stderr.write(f'Bad djlib index: {djlib_index.name} type {djlib_index.dtype}\n')
    else:
        no_track_ids_idx = djlib_index.isna()

        no_track_ids = djlib.get_df().loc[no_track_ids_idx]

        for i in range(len(no_track_ids)):
            sys.stderr.write('Track has no Rekordbox ID: %s\n' % format_track(no_track_ids.iloc[i]))
            errors += 1

        not_found_in_rekordbox = djlib.get_difference(collection)

        for i in range(len(not_found_in_rekordbox)):
            sys.stderr.write(f'Track not found in Rekordbox: {format_track(not_found_in_rekordbox)}\n')
            errors += 1

    if errors > 0:
        sys.stderr.write('djlib state is inconsistent; further operations are not safe.\n')
        return False

    return True


def djlib_values_sanity_check():
    djlib = Doc('djlib')

    errors = 0

    bad_class_tracks = djlib.get_filtered(lambda track: pd.isna(track.Class) or not re.match(r'O?[ABCX][1-5]?', track.Class))

    if len(bad_class_tracks) > 0:
        errors += 1

        print(f'{len(bad_class_tracks)} djilb tracks have a malformed Class field')
        pretty_print_tracks(bad_class_tracks, indent=' '*4, enum=True)

    return (errors == 0)

_automatic_accept_threshold = 0.9

def djlib_maintenance():
    djlib = Doc('djlib')
    main_library = RekordboxPlaylist('Main Library')

    missing_from_djlib = main_library.get_difference(djlib)
    # filter out local edits
    missing_from_djlib = dataframe_filter(missing_from_djlib,
                                          lambda track: not track['Title'].endswith('[Local Edit]'))

    djlib_auto_columns = djlib.get_df().columns.intersection(
        main_library.get_df().columns, sort=False)

    if len(missing_from_djlib) == 0:
        print('All Rekordbox Main Library tracks are in djlib')
    else:
        assert djlib.get_df().index.name in djlib_auto_columns

        print(f'{len(missing_from_djlib)} Rekordbox Main Library tracks are missing from djlib')
        pretty_print_tracks(missing_from_djlib, indent=' '*4, enum=True)

        djlib.append(missing_from_djlib[djlib_auto_columns])
        djlib.write()

    return


def rekordbox_to_spotify_maintenance(rekordbox_main_playlist='Main Library',
                                     cutoff_ratio=0.6):
    main_playlist_tracks = rekordbox.get_playlist_tracks(rekordbox_main_playlist)
    print("Rekordbox '%s' playlist: %d tracks" % (
        rekordbox_main_playlist, len(main_playlist_tracks)))

    # filter out local edits
    should_map = main_playlist_tracks.Title.apply(
        lambda title: not title.endswith('[Local Edit]')
    )
    main_playlist_tracks = main_playlist_tracks[should_map]

    rekordbox_to_spotify = docs['rekordbox_to_spotify']

    rekordbox_to_spotify_mapping = rekordbox_to_spotify.read()
    print('Rekordbox to Spotify mapping: %d entries' % len(rekordbox_to_spotify_mapping))

    assert main_playlist_tracks.index.name == 'rekordbox_id'
    assert rekordbox_to_spotify_mapping.index.name == 'rekordbox_id'

    unmapped_rekordbox_ids = main_playlist_tracks.index.difference(rekordbox_to_spotify_mapping.index, sort=False)

    if len(unmapped_rekordbox_ids) == 0:
        print('All Rekordbox main playlist tracks have Spotify mappings')
        return

    print('%d Rekordbox main playlist tracks do not have a Spotify mapping' % len(unmapped_rekordbox_ids))

    rekordbox_unmapped_tracks = main_playlist_tracks.loc[unmapped_rekordbox_ids]

    choice = get_user_choice('Look for mappings in Spotify Liked Tracks?')
    if choice == 'yes':
        spotify_liked_tracks = spotify.get_liked_tracks()

        spotify_unmapped_liked_tracks_idx = spotify_liked_tracks.index.difference(
            rekordbox_to_spotify_mapping.spotify_id, sort=False
        )

        if len(spotify_unmapped_liked_tracks_idx) == 0:
            print('All Spotify liked tracks have already been mapped')
        else:
            spotify_unmapped_liked_tracks = spotify_liked_tracks.loc[
                spotify_unmapped_liked_tracks_idx
            ]

            print('Attempting to create a mapping between %d unmapped Rekordbox tracks and '
                  '%d unmapped Spotify liked tracks' % (
                len(rekordbox_unmapped_tracks),
                len(spotify_unmapped_liked_tracks)
            ))

            rekordbox_sequences = rekordbox_unmapped_tracks.apply(format_track_for_search, axis=1)
            spotify_sequences = spotify_unmapped_liked_tracks.apply(format_track_for_search, axis=1)

            result = fuzzy_one_to_one_mapping(rekordbox_sequences.to_list(), spotify_sequences.to_list(), cutoff_ratio=cutoff_ratio)

            if len(result['pairs']) > 0:
                print('Found the following potential mappings:')

                rekordbox_mapped_ids = []

                for mapping in result['pairs']:
                    rekordbox_track = rekordbox_unmapped_tracks.iloc[mapping['index1']]
                    spotify_track = spotify_unmapped_liked_tracks.iloc[mapping['index2']]
                    print()
                    print('Rekordbox: %s' % format_track(rekordbox_track))
                    print('Spotify: %s' % format_track(spotify_track))
                    print('Match ratio: %.2f' % mapping['ratio'])
                    if mapping['ratio'] >= _automatic_accept_threshold:
                        print('Accepted automatically')
                        choice = 'yes'
                    else:
                        choice = get_user_choice('Accept?')

                    if choice == 'yes':
                        mapping_row = pd.Series({
                            'rekordbox_id': rekordbox_track.rekordbox_id,
                            'spotify_id': spotify_track.id
                        },
                        index = rekordbox_to_spotify_mapping.columns)

                        mapping_row[rekordbox_to_spotify_mapping.columns[2:]] =\
                            spotify_track[rekordbox_to_spotify_mapping.columns[2:]]

                        rekordbox_to_spotify_mapping.loc[rekordbox_track.rekordbox_id] = mapping_row
                        rekordbox_mapped_ids.append(rekordbox_track.rekordbox_id)

            choice = get_user_choice('Proceed with rekordbox to spotify mapping?')
            if choice == 'yes':
                rekordbox_to_spotify.write(rekordbox_to_spotify_mapping)
                rekordbox_unmapped_tracks = rekordbox_unmapped_tracks.loc[
                    rekordbox_unmapped_tracks.index.difference(
                        rekordbox_mapped_ids, sort=False)
                ]

    if len(rekordbox_unmapped_tracks) > 0:
        print('%d Rekordbox main playlist tracks are still unmapped' % len(rekordbox_unmapped_tracks))
        pretty_print_tracks(rekordbox_unmapped_tracks, indent=' '*4, enum=True)
        choice = get_user_choice('Do Spotify search?')
        if choice == 'yes':
            rekordbox_ids = list(rekordbox_unmapped_tracks.index)

            for rekordbox_id in rekordbox_ids:
                rekordbox_track = rekordbox_unmapped_tracks.loc[rekordbox_id]
                print('Searching for Rekordbox track: %s' % format_track(rekordbox_track))

                search_string = format_track_for_search(rekordbox_track)

                spotify_tracks = spotify.search(search_string)

                done = False
                if len(spotify_tracks) == 0:
                    print('No search results from Spotify!')
                else:
                    for i in range(len(spotify_tracks)):
                        spotify_track = spotify_tracks.iloc[i]
                        print('Option %d: %s' % (i+1, format_track(spotify_track)))
                        choice = get_user_choice('Accept?', options=['yes', 'next','give up'])
                        if choice == 'yes':
                            mapping_row = pd.Series({
                                'rekordbox_id': rekordbox_track.rekordbox_id,
                                'spotify_id': spotify_track.id
                            },
                                index=rekordbox_to_spotify_mapping.columns)

                            mapping_row[rekordbox_to_spotify_mapping.columns[2:]] = \
                                spotify_track[rekordbox_to_spotify_mapping.columns[2:]]

                            rekordbox_to_spotify_mapping.loc[rekordbox_track.rekordbox_id] = mapping_row

                            rekordbox_unmapped_tracks = rekordbox_unmapped_tracks.drop(rekordbox_track.rekordbox_id)

                            done = True
                            break
                        elif choice == 'next':
                            continue
                        elif choice == 'give up':
                            break

                if not done:
                    print('No Spotify mapping found for Rekordbox track: %s' % format_track(rekordbox_track))
                    choice = get_user_choice('Mark as not found?')
                    if choice == 'yes':
                        mapping_row = pd.Series({
                            'rekordbox_id': rekordbox_track.rekordbox_id,
                            'spotify_id': spotify_track.id
                        },
                            index=rekordbox_to_spotify_mapping.columns)

                        rekordbox_to_spotify_mapping.loc[rekordbox_track.rekordbox_id] = mapping_row

                        rekordbox_unmapped_tracks = rekordbox_unmapped_tracks.drop(rekordbox_track.rekordbox_id)

            rekordbox_to_spotify.write(rekordbox_to_spotify_mapping)

    if len(rekordbox_unmapped_tracks) == 0:
        print('All Rekordbox main playlist tracks have been mapped to Spotify!')
    else:
        print('%d Rekordbox main playlist tracks remain unmapped' % len(rekordbox_unmapped_tracks))
        pretty_print_tracks(rekordbox_unmapped_tracks, indent=' '*4, enum=True)

    return


# def djlib_spotify_likes_maintenance():
#     """
#     Rules:
#     - All tracks of classes A and B should be liked
#     - All tracks of class C should NOT be liked
#     - Classes X, P etc. can go either way
#     """
#
#     print('Checking djlib classes against Spotify liked tracks...')
#
#     djlib = docs['djlib']
#     djlib_tracks = djlib.read()
#
#     djlib_tracks_with_spotify_id = add_spotify_ids(djlib_tracks, include_missing_ids=False)
#
#     djlib_tracks_without_spotify_id = len(djlib_tracks) - len(djlib_tracks_with_spotify_id)
#     if djlib_tracks_without_spotify_id > 0:
#         print(f'{djlib_tracks_without_spotify_id} out of {len(djlib_tracks)} tracks are missing Spotify IDs; omitting.')
#
#     djlib_tracks_ab_filter = djlib_tracks_with_spotify_id.apply(
#         lambda track: is_class(track, 'A', 'B'),
#         axis=1
#     )
#     djlib_tracks_ab = djlib_tracks_with_spotify_id.loc[djlib_tracks_ab_filter]
#     print(f'{len(djlib_tracks_ab)} AB tracks...')
#
#     djlib_tracks_c_filter = djlib_tracks_with_spotify_id.apply(
#         lambda track: is_class(track, 'C'),
#         axis=1
#     )
#     djlib_tracks_c = djlib_tracks_with_spotify_id.loc[djlib_tracks_c_filter]
#     print(f'{len(djlib_tracks_c)} C tracks...')
#
#     print('Getting Spotify liked tracks...')
#     spotify_liked_tracks = spotify.get_liked_tracks()
#
#     djlib_liked_tracks = djlib_tracks_with_spotify_id.merge(
#         right=spotify_liked_tracks,
#         how='inner',
#         left_on='spotify_id',
#         right_index=True
#     )
#     djlib_liked_tracks_idx = djlib_liked_tracks.index
#
#     liked_djlib_tracks_c = djlib_tracks_c.loc[djlib_tracks_c.index.intersection(djlib_liked_tracks_idx)]
#
#     if len(liked_djlib_tracks_c) > 0:
#         print(f'{len(liked_djlib_tracks_c)} C tracks are liked on Spotify:')
#         pretty_print_tracks(liked_djlib_tracks_c, indent=' '*4, enum=True)
#
#         choice = get_user_choice('What to do?', options=['unlike', 'abort'])
#         if choice == 'abort':
#             return False
#         elif choice == 'unlike':
#             spotify.remove_liked_tracks(liked_djlib_tracks_c.spotify_id)
#             print(f'Removed {len(liked_djlib_tracks_c)} C tracks from Spotify liked tracks.')
#         else:
#             assert False
#
#     unliked_djlib_tracks_ab = djlib_tracks_ab.loc[djlib_tracks_ab.index.difference(djlib_liked_tracks_idx)]
#
#     if len(unliked_djlib_tracks_ab) > 0:
#         print(f'{len(unliked_djlib_tracks_ab)} A and B tracks are not liked on Spotify:')
#         pretty_print_tracks(unliked_djlib_tracks_ab, indent=' '*4, enum=True)
#
#         choice = get_user_choice('What to do?', options=['like', 'abort'])
#         if choice == 'abort':
#             return False
#         elif choice == 'like':
#             spotify.add_liked_tracks(unliked_djlib_tracks_ab.spotify_id)
#             print(f'Added {len(unliked_djlib_tracks_ab)} A and B tracks to Spotify liked tracks.')
#         else:
#             assert False
#
#     return True



# def library_maintenance():
#     if not rekordbox_sanity_checks():
#         return False
#
#     if not djlib_sanity_checks():
#         return False
#
#     if not djlib_values_sanity_check():
#         return False
#
#     djlib_maintenance()
#
#     rekordbox_to_spotify_maintenance()
#
#     if not djlib_spotify_likes_maintenance():
#         return
#
#     choice = get_user_choice('Rebuild Rekordbox playlists?')
#     rebuild_rekordbox = (choice == 'yes')
#
#     choice = get_user_choice('Rebuild Spotify playlists?')
#     rebuild_spotify = (choice == 'yes')
#
#     if rebuild_rekordbox or rebuild_spotify:
#         playlist_maintenance(
#             do_rekordbox=rebuild_rekordbox,
#             do_spotify=rebuild_spotify
#         )
#
#     return


