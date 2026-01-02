
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
        remove_from_disk_queue=False,
        add_to_listening_history=True
    )

    main_playlist_cont = SpotifyPlaylist(main_playlist)
    main_playlist_cont.append(listened_tracks, prompt=False)
    main_playlist_cont.write()

    return

def queue_maintenance_prog(last_track=None):
    queue_maintenance(
        last_track=last_track,
        disk_queue='prog_queue',
        spotify_queues=['prog L1', 'prog L2', 'prog L3', 'prog L4']
    )

def queue_maintenance_salsa(last_track=None):
    queue_maintenance(
        last_track=last_track,
        disk_queue='salsa_queue',
        spotify_queues=['salsa L1', 'salsa L2', 'salsa L3']
    )
