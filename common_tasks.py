from queue_workflow import *

def like_tracks_from_text_file(text_file):
    fh = open(text_file)
    lines = fh.readlines()
    fh.close()

    track_ids = []

    line_no = 0
    for line in lines:
        line_no += 1

        line = line.strip()
        if line == '':
            continue

        # find track ID
        m = re.search(r'[0-9A-Za-z]{20,}', line)
        if not m:
            raise Exception(f"Line {line_no}: can't find track ID")

        track_id = m.group(0)
        track_ids.append(track_id)

    print(f'Liking {len(track_ids)} tracks...')

    djlib_config.spotify.add_liked_tracks(track_ids)
    return

    return


def queue_maintenance_prog(last_track=None, **kwargs):
    queue_maintenance(
        last_track=last_track,
        disk_queue='prog_queue',
        spotify_queues=['prog L1', 'prog L2', 'prog L3', 'prog L4'],
        **kwargs
    )

def replenish_spotify_queue_prog(target_size=200):
    return replenish_spotify_queue(playlist_name='prog L1', queue_name='prog_queue',
                                   target_size=target_size)

def queue_maintenance_salsa(last_track=None, **kwargs):
    return queue_maintenance(
        last_track=last_track,
        disk_queue='salsa_queue',
        spotify_queues=['salsa L1', 'salsa L2', 'salsa L3', 'salsa L4', 'salsa non playable'],
        **kwargs
    )

def replenish_spotify_queue_salsa(target_size=200):
    return replenish_spotify_queue(playlist_name='salsa L1', queue_name='salsa_queue',
                                   target_size=target_size)

def salsa_playlist():
    create_spotify_playlist_from_rekordbox_playlist('Salsa DJ Library', ['managed', 'Salsa AB'])
    return



def queue_maintenance_songs(last_track=None, **kwargs):
    return queue_maintenance(
        last_track=last_track,
        # side_playlist='songs side pocket',
        disk_queue=None,
        spotify_queues=['songs L1', 'songs L2', 'songs L3', 'songs L4'],
        **kwargs
    )


def _reform_l2_playlist(playlist_names, l2_playlist_name):
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

    _reform_l2_playlist(lockwood_beethoven_playlists, 'Lockwood Beethoven L2')
    _reform_l2_playlist(lockwood_others_playlists, 'Lockwood Others L2')

    return
