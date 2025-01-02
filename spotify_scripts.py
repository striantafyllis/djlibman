
import random

import djlib_config
from containers import *

def sanity_check_disk_queues():
    queue = Doc('queue')
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
            for i in range(len(playlist_tracks)):
                if playlist_tracks.iloc[i]['name'].upper().startswith(last_listened_track.upper()):
                    last_listened_track_idx = i
                    break
            if last_listened_track_idx is None:
                raise Exception(f"Track '{last_listened_track}' not found in playlist")

        return playlist_tracks.iloc[:(last_listened_track_idx+1)]

    raise Exception(f"Invalid type for last_listened_track: {type(last_listened_track)}")


def promote_tracks_in_spotify_queues(
        last_track,
        promote_queue_name=None,
        promote_target_name=None):

    if promote_queue_name is None:
        promote_queue_level = 1
        promote_queue_name = djlib_config.get_default_spotify_queue_at_level(1)
    else:
        promote_queue_level = djlib_config.get_spotify_queue_level(promote_queue_name)

    if promote_target_name is None:
        promote_target_name = djlib_config.get_default_spotify_queue_at_level(promote_queue_level + 1)
    else:
        promote_target_level = djlib_config.get_spotify_queue_level(promote_target_name)
        if promote_target_level != promote_queue_level + 1:
            raise ValueError(f"Promote queue '{promote_queue_name}' is at level {promote_queue_level} "
                             f"but promote target '{promote_target_name}' is at level {promote_target_level}")

    promote_queue = SpotifyPlaylist(promote_queue_name)
    promote_target = SpotifyPlaylist(promote_target_name)

    listened_tracks = get_playlist_listened_tracks(promote_queue, last_track)

    print(f'{promote_queue_name}: {len(listened_tracks)} listened tracks')
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

    print(f'{promote_queue_name}: {len(listened_liked_tracks)} of the '
          f'{len(listened_tracks)} listened tracks are liked')
    pretty_print_tracks(listened_liked_tracks, indent=' ' * 4, enum=True)
    print()

    if len(listened_liked_tracks) > 0:
        print(f'Adding liked tracks to {promote_target_name}...')
        promote_target.append(listened_liked_tracks)
        promote_target.write()

        print(f'Removing from Spotify liked tracks...')
        liked.remove(listened_liked_tracks)
        liked.write()

    print(f'Removing listened tracks from {promote_queue_name}...')
    promote_queue.remove(listened_tracks)
    promote_queue.write()

    if promote_queue_level == 1:
        print(f'Removing listened tracks from disk queue...')
        queue = Doc('queue')
        queue.remove(listened_tracks)
        queue.write()

    print(f'Adding listened tracks to listening history...')
    listening_history = Doc('listening_history')
    listening_history.append(listened_tracks)
    listening_history.write()

    return

def replenish_spotify_queue(
        queue_name,
        target_size=150):
    spotify_queue = SpotifyPlaylist(queue_name)
    disk_queue = Doc('queue')

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
        pretty_print_tracks(tracks_wanted, indent=' '*4, enum=True)

        spotify_queue.append(tracks_to_add)
        spotify_queue.write()

    return

def sanity_check_spotify_queue(spotify_queue_name, is_level_1=False):
    spotify_queue = SpotifyPlaylist(spotify_queue_name)
    print(f'{spotify_queue_name}: {len(spotify_queue)} tracks')

    if len(spotify_queue) == 0:
        return

    # Make sure items in the queue are unique
    spotify_queue.deduplicate()

    if is_level_1:
        # Make sure all items in the L1 queue are also in the disk queue
        disk_queue = Doc('queue')

        tracks_not_in_disk_queue = spotify_queue.get_difference(disk_queue)

        if len(tracks_not_in_disk_queue) > 0:
            print(f'WARNING: {len(tracks_not_in_disk_queue)} tracks are in {spotify_queue_name} '
                  f'but not in disk queue')
            choice = get_user_choice('Remove?')
            if choice == 'yes':
                spotify_queue.remove(tracks_not_in_disk_queue)
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
            spotify_queue.remove(queue_tracks_in_library)

    # Make sure all items in the queue are not liked
    spotify_liked = SpotifyLiked()

    queue_liked_tracks = spotify_queue.get_intersection(spotify_liked)

    if len(queue_liked_tracks) > 0:
        print(f'WARNING: {len(queue_liked_tracks)} {spotify_queue_name} tracks are already liked')
        pretty_print_tracks(queue_liked_tracks, indent=' '*4, enum=True)
        print()

        choice = get_user_choice('Unlike?')
        if choice == 'yes':
            spotify_liked.remove(queue_liked_tracks)
            spotify_liked.write()

    spotify_queue.write()

    return


def queue_maintenance(
        last_track=None,
        promote_queue_name=None,
        promote_target_name=None
):
    # Sanity check! Queue and listening history must be disjoint
    sanity_check_disk_queues()

    if last_track is None:
        if promote_queue_name is not None or promote_target_name is not None:
            raise ValueError('promote_queue or promote_target is specified without last_track')
    else:
        promote_tracks_in_spotify_queues(last_track, promote_queue_name, promote_target_name)

    shazam = SpotifyPlaylist('My Shazam Tracks', create=True)
    if len(shazam) > 0:
        choice = get_user_choice(f'Move {len(shazam)} tracks from My Shazam Tracks to L2 queue?')
        if choice == 'yes':
            l2_queue_name = djlib_config.get_default_spotify_queue_at_level(2)
            l2_queue = SpotifyPlaylist(l2_queue_name)
            l2_queue.append(shazam)
            shazam.truncate()

            l2_queue.write()
            shazam.write()

    for i, level in enumerate(djlib_config.spotify_queues):
        for spotify_queue in level:
            sanity_check_spotify_queue(spotify_queue, i==0)

    return

def pretty_print_spotify_playlist(playlist_name):
    spotify_playlist = SpotifyPlaylist(playlist_name)

    print(f"Spotify playlist '{playlist_name}': {len(spotify_playlist)} tracks")
    pretty_print_tracks(spotify_playlist.get_df(), enum=True)
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

    queue = Doc('queue', index_name='id')
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
    queue = Doc('queue', index_name='id')

    playlist.remove(listening_history)
    playlist.remove(queue)
    playlist.write()

    return


def get_artists(tracks: Union[Container, pd.DataFrame]):
    """Returns a dictionary - id to artist name - from the argument - which
       should be a DF of Spotify tracks or similar."""

    if isinstance(tracks, Container):
        tracks = tracks.get_df()

    artist_id_to_name = {}

    for artist_list in tracks.artists:
        if not isinstance(artist_list, list) and pd.isna(artist_list):
            continue
        for artist in artist_list:
            id = artist['id']
            name = artist['name']

            existing_name = artist_id_to_name.get(id)
            if existing_name is None:
                artist_id_to_name[id] = name
            elif existing_name != name:
                # Sometimes this happens if an artist changes their Spotify name.
                # In that case, keep the latest name.
                print(f'Warning: Artist ID {id} associated with two different names: '
                      f'{existing_name} and {name}')
                artist_id_to_name[id] = name

    return artist_id_to_name


def find_spotify_artist(artist_name):
    """Returns the Spotify entry for the artist with the given name as a dictionary.
    The ID is in there also.
    The search is first done in the library, then expands to all of Spotify if necessary."""

    listening_history = Doc('listening_history')

    artists = get_artists(listening_history)

    candidate_ids = []

    for id, name in artists.items():
        if name == artist_name:
            candidate_ids.append(id)

    if len(candidate_ids) > 1:
        raise ValueError(f'Multiple IDs found for artist {artist_name}: {candidate_ids}')
    elif len(candidate_ids) == 0:
        raise ValueError(f'Artist {artist_name} not found')

    return candidate_ids[0]

def has_artist(track, artist_id):
    for artist in track.artists:
        if artist['id'] == artist_id:
            return True
    return False


def get_artist_discography(artist_name):
    artist_id = find_spotify_artist(artist_name)

    artist_albums = djlib_config.spotify.get_artist_albums(artist_id)

    num_tracks = sum([
        album['total_tracks'] for album in artist_albums
    ])

    print(f'{artist_name}: found {len(artist_albums)} albums with {num_tracks} tracks')
    # pretty_print_albums(artist_albums, indent=' '*4, enum=True)

    tracks = None

    for album in artist_albums:
        album_tracks = djlib_config.spotify.get_album_tracks(album['id'])

        artist_album_tracks = album_tracks.loc[album_tracks.apply(lambda t: has_artist(t, artist_id), axis=1)]

        print(f'Album {album["name"]}: {len(album_tracks)} tracks, {len(artist_album_tracks)} artist tracks')

        if tracks is None:
            tracks = artist_album_tracks
        else:
            tracks = pd.concat([tracks, artist_album_tracks])

    print(f'{artist_name}: found {len(tracks)} tracks')

    mixed_tracks = tracks.apply(lambda t: t['name'].endswith(' - Mixed'), axis=1)

    tracks = tracks.loc[~mixed_tracks]

    print(f'{artist_name}: after removing mixed tracks: {len(tracks)} tracks')

    tracks = Wrapper(contents=tracks, name=f'Discography for artist {artist_name}')
    tracks.sort('popularity', ascending=False)
    tracks.deduplicate(deep=True)

    print(f'{artist_name}: after deduplication: {len(tracks)} tracks')

    return tracks


def sample_artist_to_queue(artist_name, latest=10, popular=10):
    print(f'Sampling artist {artist_name} to queue...')
    discogs = get_artist_discography(artist_name)

    print(f'Found {len(discogs)} tracks')

    listening_history = Doc('listening_history')
    queue = Doc('queue')

    discogs.remove(listening_history)
    discogs.remove(queue)

    print(f'Left after removing listening history and queue: {len(discogs)} tracks')

    discogs.sort('added_at', ascending=False)

    latest_tracks = discogs.get_df()[:latest]

    discogs.remove(latest_tracks)

    print('Latest tracks:')
    pretty_print_tracks(latest_tracks, indent=' '*4, enum=True)

    discogs.sort('popularity', ascending=False)

    most_popular_tracks = discogs.get_df()[:popular]

    print('Most popular tracks:')
    pretty_print_tracks(most_popular_tracks, indent=' '*4, enum=True)

    queue.append(latest_tracks)
    queue.append(most_popular_tracks)

    queue.write()

    return

