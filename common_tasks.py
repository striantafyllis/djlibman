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

    add_liked_tracks(track_ids)
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

def queue_maintenance_great_american_songbook(last_track=None, **kwargs):
    return queue_maintenance(
        last_track=last_track,
        disk_queue=None,
        spotify_queues=['Great American Songbook L1', 'Great American Songbook L2'],
        **kwargs
    )

great_american_songbook_csv = '/Users/spyros/Music/djlib/Great American Songbook with Spotify IDs.csv'

def great_american_songbook_to_songs_queue(use_listening_history=True):
    great_american_songbook = Doc(
        name='Great American Songbook',
        path=great_american_songbook_csv,
        index_column='spotify_id'
    )

    great_american_songs = great_american_songbook.get_df()

    # throw away the songs without a spotify ID; we can't handle these yet
    great_american_songs = great_american_songs[great_american_songs.index.notna()]

    # for each song, find the one with the max popularity score

    groups = great_american_songs.groupby(['Year', 'Song title', 'Composer(s)'])

    great_american_songs_max_pop = groups.apply(lambda group: group.loc[group['match_score'].idxmax()])

    print(f'Found {len(great_american_songs_max_pop)} unique songs in Great American Songbook')

    if use_listening_history:
        listening_history = ListeningHistory()

        # remove the songs in listening history
        unlistened_great_american_songs_max_pop = great_american_songs_max_pop.loc[
            ~great_american_songs_max_pop.index.isin(listening_history.get_df().index)
        ]
        print(f'{len(unlistened_great_american_songs_max_pop)} of these songs are unlistened')

        great_american_songs_max_pop = unlistened_great_american_songs_max_pop

    print(f'Adding {len(great_american_songs_max_pop)} songs to queue...')

    queue = SpotifyPlaylist('Great American Songbook L1')

    queue.append(great_american_songs_max_pop)

    print(f'songs L1 now has {len(queue)} songs')

    queue.write()
    return

