
import random

import spotify_discography
from containers import *

def sanity_check_disk_queues():
    queue = Queue()
    print(f'Queue: {len(queue)} tracks')

    listening_history = Doc('listening_history')
    print(f'Listening history: {len(listening_history)} tracks')

    library = RekordboxPlaylist('Main Library')
    print(f'Library: {len(library)} tracks')

    # entries in the queue should be unique
    queue.deduplicate()

    # entries in listening history should be unique
    listening_history.deduplicate()

    # queue tracks should not be in listening history
    queue.remove(listening_history)

    # library tracks should be in listening history and should not be in queue
    listening_history.append(library)
    queue.remove(library)

    queue.write()
    listening_history.write()

    return


def get_playlist_listened_tracks(
        playlist: SpotifyPlaylist,
        last_listened_track) -> pd.DataFrame:

    playlist_tracks = playlist.get_df()

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
            candidate_tracks = []
            for i in range(len(playlist_tracks)):
                if playlist_tracks.iloc[i]['name'].upper().startswith(last_listened_track.upper()):
                    candidate_tracks.append(i)
                    break
            if len(candidate_tracks) > 1:
                raise Exception(f"Last listened track {last_listened_track} is ambiguous; "
                                f"{len(candidate_tracks)} matches")
            elif len(candidate_tracks) == 0:
                raise Exception(f"Track '{last_listened_track}' not found in playlist")
            else:
                last_listened_track_idx = candidate_tracks[0]

        return playlist_tracks.iloc[:(last_listened_track_idx+1)]

    raise Exception(f"Invalid type for last_listened_track: {type(last_listened_track)}")


def promote_tracks_in_spotify_queue(
        last_track,
        promote_source_name=None,
        promote_target_name=None):

    if promote_source_name is None:
        promote_queue_level = 1
        promote_source_name = djlib_config.get_default_spotify_queue_at_level(1)
    else:
        promote_queue_level = djlib_config.get_spotify_queue_level(promote_source_name)

    if promote_target_name is None:
        promote_target_name = djlib_config.get_default_spotify_queue_at_level(promote_queue_level + 1)
    else:
        promote_target_level = djlib_config.get_spotify_queue_level(promote_target_name)
        if promote_target_level != promote_queue_level + 1:
            raise ValueError(f"Promote queue '{promote_source_name}' is at level {promote_queue_level} "
                             f"but promote target '{promote_target_name}' is at level {promote_target_level}")

    promote_queue = SpotifyPlaylist(promote_source_name)
    promote_target = SpotifyPlaylist(promote_target_name)

    listened_tracks = get_playlist_listened_tracks(promote_queue, last_track)

    print(f'{promote_source_name}: {len(listened_tracks)} listened tracks')
    pretty_print_tracks(listened_tracks, indent=' ' * 4, enum=True)
    choice = get_user_choice('Is this correct?')
    if choice != 'yes':
        return

    if len(listened_tracks) == 0:
        return

    # find how many of the listened tracks are liked
    liked = SpotifyLiked()

    listened_liked_tracks_idx = listened_tracks.index.intersection(liked.get_df().index, sort=False)
    listened_liked_tracks = listened_tracks.loc[listened_liked_tracks_idx]

    print(f'{promote_source_name}: {len(listened_liked_tracks)} of the '
          f'{len(listened_tracks)} listened tracks are liked')
    pretty_print_tracks(listened_liked_tracks, indent=' ' * 4, enum=True)
    choice = get_user_choice('Is this correct?')
    if choice != 'yes':
        return
    print()

    if len(listened_liked_tracks) > 0:
        promote_target.append(listened_liked_tracks, prompt=False)
        promote_target.write()

        liked.remove(listened_liked_tracks, prompt=False)
        liked.write()

    promote_queue.remove(listened_tracks, prompt=False)
    promote_queue.write()

    if promote_queue_level == 1:
        print(f'Removing listened tracks from disk queue...')
        queue = Queue()
        queue.remove(listened_tracks, prompt=False)
        queue.write()

    print(f'Adding listened tracks to listening history...')
    listening_history = Doc('listening_history')
    listening_history.append(listened_tracks, prompt=False)
    listening_history.write()

    return

def replenish_spotify_queue(
        queue_name='L1 queue',
        target_size=200):
    spotify_queue = SpotifyPlaylist(queue_name)
    disk_queue = Queue()

    tracks_wanted = target_size - len(spotify_queue)
    if tracks_wanted <= 0:
        print(f'Spotify playlist {queue_name} already has {len(spotify_queue)} tracks; no replenishment needed.')
        return

    candidate_tracks_idx = disk_queue.get_df().index.difference(spotify_queue.get_df().index)

    if len(candidate_tracks_idx) == 0:
        print(f'Disk queue has no other tracks; no replenishment possible.')
        return

    num_tracks_to_add = min(tracks_wanted, len(candidate_tracks_idx))

    choice = get_user_choice(f'Add {tracks_wanted} new tracks to {queue_name}?')
    if choice == 'yes':
        if num_tracks_to_add < len(candidate_tracks_idx):
            tracks_to_add_idx = random.sample(candidate_tracks_idx.to_list(), k=num_tracks_to_add)
            tracks_to_add = disk_queue.get_df().loc[tracks_to_add_idx]
        else:
            tracks_to_add = disk_queue.get_df().loc[candidate_tracks_idx]

        print(f'Adding {num_tracks_to_add} tracks to {queue_name}')
        pretty_print_tracks(tracks_to_add, indent=' '*4, enum=True)

        spotify_queue.append(tracks_to_add)
        spotify_queue.write()

    return

def sanity_check_spotify_queue(spotify_queue_name, *, is_level_1=False, is_promote_queue=False):
    spotify_queue = SpotifyPlaylist(spotify_queue_name)
    print(f'{spotify_queue_name}: {len(spotify_queue)} tracks')

    if len(spotify_queue) == 0:
        return

    # Make sure items in the queue are unique
    spotify_queue.deduplicate()

    if is_level_1:
        pass
    else:
        # Make sure all items in the L2+ queues are already in the listening history
        listening_history = Doc('listening_history')
        listening_history.append(spotify_queue)
        listening_history.write()

    # Make sure items in the queue are not already in the library
    library = RekordboxPlaylist('Main Library')

    queue_tracks_in_library = spotify_queue.get_intersection(library)
    if len(queue_tracks_in_library) > 0:
        print(f'WARNING: {len(queue_tracks_in_library)} tracks are already in the library')
        choice = get_user_choice('Remove?')
        if choice == 'yes':
            spotify_queue.remove(queue_tracks_in_library, prompt=False)

    # Make sure all items in the queue are not liked
    if not is_promote_queue:
        spotify_liked = SpotifyLiked()

        queue_liked_tracks = spotify_queue.get_intersection(spotify_liked)

        if len(queue_liked_tracks) > 0:
            print(f'WARNING: {len(queue_liked_tracks)} {spotify_queue_name} tracks are already liked')
            pretty_print_tracks(queue_liked_tracks, indent=' '*4, enum=True)
            print()

            choice = get_user_choice('Unlike?')
            if choice == 'yes':
                spotify_liked.remove(queue_liked_tracks, prompt=False)
                spotify_liked.write()

    spotify_queue.write()

    return


def queue_maintenance(
        last_track=None,
        promote_source=None,
        promote_target=None
):
    if last_track is None:
        if promote_source is not None or promote_target is not None:
            raise ValueError('promote_source or promote_target is specified without last_track')

    # Sanity check! Queue and listening history must be disjoint
    sanity_check_disk_queues()

    for i, level in enumerate(djlib_config.spotify_queues):
        for spotify_queue in level:
            sanity_check_spotify_queue(spotify_queue,
                                       is_level_1=(i==0),
                                       is_promote_queue=(spotify_queue == promote_source))

    if last_track is not None:
        promote_tracks_in_spotify_queue(last_track, promote_source, promote_target)

    shazam = SpotifyPlaylist('My Shazam Tracks', create=True)
    if len(shazam) > 0:
        choice = get_user_choice(f'Move {len(shazam)} tracks from My Shazam Tracks to L2 queue?')
        if choice == 'yes':
            l2_queue_name = djlib_config.get_default_spotify_queue_at_level(2)
            l2_queue = SpotifyPlaylist(l2_queue_name)
            l2_queue.append(shazam, prompt=False)
            shazam.truncate(prompt=False)

            l2_queue.write()
            shazam.write()

            sanity_check_spotify_queue(l2_queue_name, is_level_1=False)

    return

def pretty_print_spotify_playlist(playlist_name):
    spotify_playlist = SpotifyPlaylist(playlist_name)

    print(f"Spotify playlist '{playlist_name}': {len(spotify_playlist)} tracks")
    pretty_print_tracks(spotify_playlist.get_df(), enum=True, ids=False)
    return

def shuffle_spotify_playlist(playlist_name):
    playlist = SpotifyPlaylist(playlist_name)
    new_playlist = SpotifyPlaylist(playlist_name + ' - shuffled', create=True, overwrite=False)

    tracks = playlist.get_df()

    new_tracks_idx = random.sample(tracks.index.to_list(), k=len(tracks))

    new_tracks = tracks.loc[new_tracks_idx]

    new_playlist.set_df(new_tracks)
    new_playlist.write()

    return

def add_to_queue(tracks):
    """Adds tracks to the disk queue. The tracks have to be either a Container or a DataFrame."""

    if isinstance(tracks, str):
        tracks = SpotifyPlaylist(tracks)

    tracks_wrapper = Wrapper(tracks)

    print(f'Attempting to add {len(tracks_wrapper)} tracks to the disk queue...')

    if len(tracks_wrapper) == 0:
        return

    listening_history = Doc('listening_history')

    tracks_wrapper.remove(listening_history)

    if len(tracks_wrapper) == 0:
        return

    queue = Queue()
    queue.append(tracks_wrapper)
    queue.write()

    choice = get_user_choice('Add to L1 queue also?')
    if choice == 'yes':
        l1_queue_name = djlib_config.get_default_spotify_queue_at_level(1)

        l1_queue = SpotifyPlaylist(l1_queue_name)
        l1_queue.append(tracks_wrapper, prompt=False)
        l1_queue.write()

    return

def filter_spotify_playlist(playlist_name):
    """Removes tracks in queue and listening history from a Spotify playlist"""

    playlist = SpotifyPlaylist(playlist_name)

    listening_history = Doc('listening_history')
    queue = Queue()

    playlist.remove(listening_history)
    playlist.remove(queue)
    playlist.write()

    return



def sample_artist_to_queue(artist_name, *, latest=10, popular=10):
    print(f'Sampling artist {artist_name} to queue...')
    discogs = Wrapper(spotify_discography.get_artist_discography(artist_name),
                      name=f'discography for artist {artist_name}')

    print(f'Found {len(discogs)} tracks')

    listening_history = Doc('listening_history')
    queue = Queue()

    discogs.remove(listening_history, prompt=False)
    discogs.remove(queue, prompt=False)

    print(f'Left after removing listening history and queue: {len(discogs)} tracks')

    if latest > 0:
        discogs.sort('release_date', ascending=False)

        latest_tracks = discogs.get_df()[:latest]

        discogs.remove(latest_tracks, prompt=False)

        print('Latest tracks:')
        pretty_print_tracks(latest_tracks, indent=' '*4, enum=True, extra_attribs='release_date')

        queue.append(latest_tracks)

    if popular > 0:
        discogs.sort('popularity', ascending=False)

        most_popular_tracks = discogs.get_df()[:popular]

        print('Most popular tracks:')
        pretty_print_tracks(most_popular_tracks, indent=' '*4, enum=True, extra_attribs='popularity')

        queue.append(most_popular_tracks)

    queue.write()

    return

def text_file_to_spotify_tracks(text_file):
    lines = read_lines_from_file(text_file)

    search_strings = [format_track_for_search(line) for line in lines]

    return
