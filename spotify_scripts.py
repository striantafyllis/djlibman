
import random

from containers import *

def sanity_check_disk_queues():
    queue = Doc('queue')
    print(f'Queue: {len(queue.get_df())} tracks')

    queue_history = Doc('queue_history')
    print(f'Queue history: {len(queue_history.get_df())} tracks')

    library = RekordboxPlaylist('Main Library')
    print(f'Library: {len(library.get_df())} tracks')

    # entries in the queue should be unique
    queue.deduplicate()

    # entries in queue history should be unique
    queue_history.deduplicate()

    # queue tracks should not be in queue history
    queue.remove(queue_history)

    # library tracks should be in queue history and should not be in queue
    queue_history.append(library)
    queue.remove(library)

    queue.write()
    queue_history.write()

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
            for i in range(len(playlist_tracks)):
                if playlist_tracks.iloc[i]['name'].upper().startswith(last_listened_track.upper()):
                    last_listened_track_idx = i
                    break
            if last_listened_track_idx is None:
                raise Exception(f"Track '{last_listened_track}' not found in playlist")

        return playlist_tracks.iloc[:(last_listened_track_idx+1)]

    raise Exception(f"Invalid type for last_listened_track: {type(last_listened_track)}")


def promote_tracks_in_spotify_queues(
        queue1_name,
        queue2_name,
        last_track,
        is_level_1=False):
    queue1 = SpotifyPlaylist(queue1_name)
    queue2 = SpotifyPlaylist(queue2_name)

    listened_tracks = get_playlist_listened_tracks(queue1, last_track)

    print(f'{queue1_name}: {len(listened_tracks)} listened tracks')
    pretty_print_tracks(listened_tracks, indent=' ' * 4, enum=True)
    print()
    choice = get_user_choice('Is this correct?')
    if choice != 'yes':
        return

    if len(listened_tracks) == 0:
        return

    # find how many of the listened tracks are liked
    liked = SpotifyLiked()

    listened_liked_tracks_idx = listened_tracks.index.intersection(liked.get_df().index, sort=False)
    listened_liked_tracks = listened_tracks.loc[listened_liked_tracks_idx]

    print(f'{queue1_name}: {len(listened_liked_tracks)} of the '
          f'{len(listened_tracks)} listened tracks are liked')
    pretty_print_tracks(listened_liked_tracks, indent=' ' * 4, enum=True)
    print()

    if len(listened_liked_tracks) > 0:
        print(f'Adding liked tracks to {queue2_name}...')
        queue2.append(listened_liked_tracks)
        queue2.write()

        print(f'Removing from Spotify liked tracks...')
        liked.remove(listened_liked_tracks)
        liked.write()

    print(f'Removing listened tracks from {queue1_name}...')
    queue1.remove(listened_tracks)

    if is_level_1:
        print(f'Removing listened tracks from disk queue...')
        queue = Doc('queue')
        queue.remove(listened_tracks)
        queue.write()

        print(f'Adding listened tracks to queue history...')
        queue_history = Doc('queue_history')
        queue_history.append(listened_tracks)
        queue_history.write()

    return

def replenish_spotify_queue(
        queue_name,
        target_size=150):
    spotify_queue = SpotifyPlaylist(queue_name)
    disk_queue = Doc('queue')

    tracks_wanted = target_size - len(spotify_queue.get_df())
    if tracks_wanted <= 0:
        print(f'Spotify playlist {queue_name} already has {len(spotify_queue.get_df())} tracks; no replenishment needed.')
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
        pretty_print_tracks(tracks_wanted, indent=' '*4, enum=True)

        spotify_queue.append(tracks_to_add)
        spotify_queue.write()

    return

def sanity_check_spotify_queue(spotify_queue_name, is_level_1=False):
    spotify_queue = SpotifyPlaylist(spotify_queue_name)
    print(f'{spotify_queue_name}: {len(spotify_queue.get_df())} tracks')

    if is_level_1:
        # Make sure all items in the L1 queue are also in the disk queue
        disk_queue = Doc('queue')

        tracks_not_in_disk_queue_idx = spotify_queue.get_df().index.difference(
            disk_queue.get_df().index, sort=False)

        if len(tracks_not_in_disk_queue_idx) > 0:
            print(f'WARNING: {len(tracks_not_in_disk_queue_idx)} tracks are in {spotify_queue_name} '
                  f'but not in disk queue')
            choice = get_user_choice('Remove?')
            if choice == 'yes':
                spotify_queue.remove(tracks_not_in_disk_queue_idx)
                spotify_queue.write()

    # Make sure items in the L1 queue are unique
    spotify_queue.deduplicate()

    # Make sure all items in the L1 queue are not liked
    spotify_liked = SpotifyLiked()

    queue_liked_tracks = spotify_queue.get_intersection(spotify_liked)

    if len(queue_liked_tracks) > 0:
        print(f'WARNING: {len(queue_liked_tracks)} {spotify_queue_name} tracks are already liked')
        pretty_print_tracks(queue_liked_tracks, indent=' '*4, enum=True)
        print()

        choice = get_user_choice('Unlike?')
        if choice == 'yes':
            spotify_liked.remove(queue_liked_tracks)

    spotify_queue.write()

    return


def queue_maintenance():
    # TODO continue here

    # Sanity check! Queue and queue history must be disjoint
    sanity_check_disk_queues()

    # move_l1_queue_listened_tracks_to_l2(
    #     l1_queue_name=l1_queue_name,
    #     l2_queue_name=l2_queue_name,
    #     l1_queue_last_listened_track=last_track
    # )
    #
    # replenish_l1_queue(
    #     l1_queue_name=l1_queue_name,
    #     target_size=l1_queue_target_size)
    #
    # add_shazam_to_l2_queue(
    #     shazam_name=shazam_name,
    #     l2_queue_name=l2_queue_name)
    #
    # sanity_check_l1_queue(
    #     l1_queue_name=l1_queue_name)
    #
    # sanity_check_l2_queue(
    #     l2_queue_name=l2_queue_name)

    return

def pretty_print_spotify_playlist(playlist_name):
    spotify_playlist = SpotifyPlaylist(playlist_name)

    print(f"Spotify playlist '{playlist_name}': {len(spotify_playlist.get_df())} tracks")
    pretty_print_tracks(spotify_playlist.get_df(), enum=True)
    return

def shuffle_spotify_playlist(playlist_name):
    # tracks = spotify.get_playlist_tracks(playlist_name)
    #
    # new_tracks = random.sample(tracks.index.to_list(), k=len(tracks))
    #
    # spotify_playlists = spotify.get_playlists()
    #
    # new_playlist_name = playlist_name + ' - shuffled'
    #
    # while new_playlist_name in spotify_playlists.index:
    #     new_playlist_name += ' - shuffled'
    #
    # spotify.create_playlist(new_playlist_name)
    # spotify.add_tracks_to_playlist(new_playlist_name, new_tracks)
    #
    # print(f"Created Spotify playlist '{new_playlist_name}' with {len(new_tracks)} tracks")

    return
