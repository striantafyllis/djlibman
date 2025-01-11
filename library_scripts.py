
import pandas as pd
import numpy as np

from djlib_config import *
from general_utils import *
from containers import *
import classification


def add_spotify_fields_to_rekordbox(rekordbox_tracks: pd.DataFrame, *, drop_missing_ids=False):
    rekordbox_to_spotify = Doc('rekordbox_to_spotify')

    rekordbox_to_spotify_df = rekordbox_to_spotify.get_df()

    if drop_missing_ids:
        rekordbox_to_spotify_df = dataframe_filter(
            rekordbox_to_spotify_df,
            lambda track: not pd.isna(track['spotify_id'])
        )

    if rekordbox_tracks.index.name != 'rekordbox_id':
        raise ValueError('Argument is not indexed by rekordbox_id')

    rekordbox_tracks_with_spotify_fields = rekordbox_tracks.merge(
        right=rekordbox_to_spotify_df,
        left_index=True,
        right_index=True,
        how=('inner' if drop_missing_ids else 'left')
    )

    return rekordbox_tracks_with_spotify_fields


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

    bad_class_tracks = djlib.get_filtered(
        lambda track: not pd.isna(track.Class) and
                      not re.match(r'[?O]?[ABCX][1-5]?', track.Class))

    if len(bad_class_tracks) > 0:
        errors += 1

        print(f'{len(bad_class_tracks)} djlib tracks have a malformed Class field')
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
    main_library = RekordboxPlaylist('Main Library')
    print(f'Rekordbox main library: {len(main_library)} tracks')

    rekordbox_to_spotify = Doc('rekordbox_to_spotify')
    print(f'Rekordbox to Spotify mapping: {len(rekordbox_to_spotify)} entries')

    unmapped_rekordbox_tracks = main_library.get_difference(rekordbox_to_spotify)

    # filter out local edits
    unmapped_rekordbox_tracks = dataframe_filter(unmapped_rekordbox_tracks,
                                  lambda track: not track['Title'].endswith('[Local Edit]'))

    if len(unmapped_rekordbox_tracks) == 0:
        print('All Rekordbox main playlist tracks have Spotify mappings')
        return

    print(f'{len(unmapped_rekordbox_tracks)} Rekordbox main playlist tracks do not have a Spotify mapping')

    # look for mappings in listening history first
    listening_history = Doc('listening_history')

    unmapped_listened_tracks = listening_history.get_difference(
        pd.Index(rekordbox_to_spotify.get_df().spotify_id))

    # handy hack to create a new dataframe with the same schema as the existing one
    new_mappings = pd.DataFrame(rekordbox_to_spotify.get_df().iloc[:0])

    if len(unmapped_listened_tracks) == 0:
        print('All listening history tracks have already been mapped')
    else:
        print(f'Attempting to create a mapping between {len(unmapped_rekordbox_tracks)} unmapped Rekordbox '
              f'tracks and {len(unmapped_listened_tracks)} unmapped listening history tracks')

        rekordbox_sequences = unmapped_rekordbox_tracks.apply(format_track_for_search, axis=1)
        listened_sequences = unmapped_listened_tracks.apply(format_track_for_search, axis=1)

        result = fuzzy_one_to_one_mapping(rekordbox_sequences.to_list(), listened_sequences.to_list(),
                                          cutoff_ratio=cutoff_ratio)

        for mapping in result['pairs']:
            rekordbox_idx = mapping['index1']
            listened_idx = mapping['index2']
            rekordbox_track = unmapped_rekordbox_tracks.iloc[rekordbox_idx]
            spotify_track = unmapped_listened_tracks.iloc[listened_idx]
            print()
            print(f'Rekordbox: {format_track(rekordbox_track)}')
            print(f'Spotify: {format_track(spotify_track)}')
            print(f'Match ratio: {mapping['ratio']:.2f}')
            if mapping['ratio'] >= _automatic_accept_threshold:
                print('Accepted automatically')
                choice = 'yes'
            else:
                choice = get_user_choice('Accept?')

            if choice == 'yes':
                rekordbox_id = rekordbox_track['rekordbox_id']

                new_mappings.loc[rekordbox_id] = { 'rekordbox_id': rekordbox_id} |\
                    spotify_track[new_mappings.columns[1:]].to_dict()

    unmapped_rekordbox_tracks = unmapped_rekordbox_tracks.loc[
        unmapped_rekordbox_tracks.index.difference(new_mappings.index, sort=False)
    ]

    if len(unmapped_rekordbox_tracks) > 0:
        print(f'{len(unmapped_rekordbox_tracks)} Rekordbox main playlist tracks are still unmapped:')
        pretty_print_tracks(unmapped_rekordbox_tracks, indent=' '*4, enum=True)
        choice = get_user_choice('Do Spotify search?')
        if choice == 'yes':
            for rekordbox_track in unmapped_rekordbox_tracks.values:
                rekordbox_id = rekordbox_track['rekordbox_id']

                print(f'Searching for Rekordbox track: {format_track(rekordbox_track)}')

                search_string = format_track_for_search(rekordbox_track)

                spotify_tracks = spotify.search(search_string)

                done = False
                if len(spotify_tracks) == 0:
                    print('No search results from Spotify!')
                else:
                    for i in range(len(spotify_tracks)):
                        spotify_track = spotify_tracks.iloc[i]
                        print(f'Option {i+1}: {format_track(spotify_track)}')
                        choice = get_user_choice('Accept?', options=['yes', 'next','give up'])
                        if choice == 'yes':
                            new_mappings.loc[rekordbox_id] = {'rekordbox_id': rekordbox_id}
                            new_mappings.loc[rekordbox_id][new_mappings.columns[1:]] = \
                                spotify_track[new_mappings.columns[1:]]

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
                        new_mappings.loc[rekordbox_id] = {'rekordbox_id': rekordbox_id}


    unmapped_rekordbox_tracks = unmapped_rekordbox_tracks.loc[
        unmapped_rekordbox_tracks.index.difference(new_mappings.index, sort=False)
    ]

    if len(unmapped_rekordbox_tracks) == 0:
        print('All Rekordbox main playlist tracks have been mapped to Spotify!')
    else:
        print(f'{len(unmapped_rekordbox_tracks)} Rekordbox main playlist tracks remain unmapped')
        pretty_print_tracks(unmapped_rekordbox_tracks, indent=' '*4, enum=True)

    if len(new_mappings) > 0:
        rekordbox_to_spotify.append(new_mappings)
        rekordbox_to_spotify.write()

    return


def djlib_spotify_likes_maintenance():
    """
    Rules:
    - All tracks of classes A and B should be liked
    - All tracks of class C should NOT be liked
    - Classes X, P etc. can go either way
    """

    print('Checking djlib classes against Spotify liked tracks...')

    djlib = Doc('djlib')

    ab_tracks = djlib.get_filtered(lambda track: classification.track_is(track, classes=['A', 'B']))
    c_tracks = djlib.get_filtered(lambda track: classification.track_is(track, classes=['C']))

    ab_tracks_with_spotify = add_spotify_fields_to_rekordbox(ab_tracks, drop_missing_ids=True)
    c_tracks_with_spotify = add_spotify_fields_to_rekordbox(c_tracks, drop_missing_ids=True)

    spotify_liked = SpotifyLiked()

    spotify_liked.append(Wrapper(ab_tracks_with_spotify, name='A and B library tracks'))
    spotify_liked.remove(Wrapper(c_tracks_with_spotify, name='C library tracks'))

    spotify_liked.write()
    return

def playlists_maintenance(do_rekordbox=True, do_spotify=True):
    djlib = Doc('djlib')

    groups = classification.classify_tracks(djlib.get_df())

    for name, group in groups.items():
        if do_rekordbox:
            rekordbox_playlist = RekordboxPlaylist(
                name=['managed'] + list(name),
                create=True,
                overwrite=True
            )
            rekordbox_playlist.set_df(group)

            print(f'Creating Rekordbox playlist {name}: {len(rekordbox_playlist)} tracks')
            rekordbox_playlist.write(force=True)

        if do_spotify and name[0] not in ['Pending', 'Old', 'CX']:
            spotify_playlist = SpotifyPlaylist(
                name=' '.join(['DJ'] + list(name)),
                create=True,
                overwrite=True
            )
            spotify_playlist.set_df(group)

            print(f'Creating Spotify playlist {name}: {len(spotify_playlist)} tracks')
            spotify_playlist.write(force=True)

    return


def library_maintenance():
    if not rekordbox_sanity_checks():
        return False

    if not djlib_sanity_checks():
        return False

    if not djlib_values_sanity_check():
        return False

    djlib_maintenance()

    rekordbox_to_spotify_maintenance()

    djlib_spotify_likes_maintenance()

    playlists_maintenance()

    return


