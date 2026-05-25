
from spotify_scripts import *

def number_ones_maintenance(
        last_track,
        *,
        temp_playlist='number ones pt2',
        main_playlist='number ones',
        next_level_playlist='number ones L2'
):
    listened_tracks = promote_tracks_in_spotify_queue(
        last_track,
        promote_source_name=temp_playlist,
        promote_target_name=next_level_playlist,
        unlike=False,
        remove_from_source=True,
        add_to_listening_history=True
    )

    main_playlist_cont = SpotifyPlaylist(main_playlist)
    main_playlist_cont.append(listened_tracks, prompt=False)
    main_playlist_cont.write()

    return

def rebuild_number_ones_l2():
    number_ones_l2 = SpotifyPlaylist('number ones L2', overwrite=True)
    number_ones = SpotifyPlaylist('number ones')
    liked = SpotifyLiked()
    number_ones_liked = number_ones.get_df().loc[
        number_ones.get_df().index.intersection(
            liked.get_df().index, sort=False)]
    number_ones_l2.set_df(number_ones_liked)
    number_ones_l2.write()
    return

def queue_maintenance_prog(last_track=None, **kwargs):
    queue_maintenance(
        last_track=last_track,
        disk_queue='prog_queue',
        spotify_queues=['prog L1', 'prog L2', 'prog L3', 'prog L4'],
        **kwargs
    )

def queue_maintenance_salsa(last_track=None, **kwargs):
    return queue_maintenance(
        last_track=last_track,
        disk_queue='salsa_queue',
        spotify_queues=['salsa L1', 'salsa L2', 'salsa L3', 'salsa L4', 'salsa non playable'],
        **kwargs
    )

def replenish_spotify_queue_prog(target_size=200):
    return replenish_spotify_queue(playlist_name='prog L1', queue_name='prog_queue',
                                   target_size=target_size)

def replenish_spotify_queue_salsa(target_size=200):
    return replenish_spotify_queue(playlist_name='salsa L1', queue_name='salsa_queue',
                                   target_size=target_size)

def salsa_playlist():
    create_spotify_playlist_from_rekordbox_playlist('Salsa DJ Library', ['managed', 'Salsa AB'])
    return

def _form_l2_playlist(playlist_names, l2_playlist_name):
    liked_idx = SpotifyLiked().get_df().index

    l2_playlist = SpotifyPlaylist(l2_playlist_name)
    l2_playlist.truncate(prompt=False)

    for playlist_name in playlist_names:
        playlist = SpotifyPlaylist(playlist_name)
        df = playlist.get_df()
        playlist_liked_idx = df.index.intersection(liked_idx, sort=False)

        print(f'Playlist {playlist_name}: {len(playlist_liked_idx)} liked tracks; '
              f'adding to {l2_playlist_name}')

        if len(playlist_liked_idx) > 0:
            playlist_liked = df.loc[playlist_liked_idx]
            l2_playlist.append(playlist_liked, prompt=False)

    l2_playlist.write()
    return


def manage_lockwood():
    spotify_playlists = djlib_config.spotify.get_playlists()

    lockwood_playlists = [name for name in spotify_playlists.name if name.startswith('Lockwood')]

    lockwood_beethoven_playlists =  [
        name
        for name in spotify_playlists.name
        if name.startswith('Lockwood Beethoven')
        and not name.endswith('L2')
    ]
    lockwood_others_playlists = [
        name
        for name in spotify_playlists.name
        if (name.startswith('Lockwood')
        and not name.startswith('Lockwood Beethoven')
           and not name.endswith('L2'))
    ]

    lockwood_beethoven_playlists.sort()
    lockwood_others_playlists.sort()

    _form_l2_playlist(lockwood_beethoven_playlists, 'Lockwood Beethoven L2')
    _form_l2_playlist(lockwood_others_playlists, 'Lockwood Others L2')

    return
