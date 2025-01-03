
import sys
from djlibman import *


def remove_artist_old_entries_from_listening_history(
        artist_name,
        cutoff_date
):
    artist_id = find_spotify_artist(artist_name)

    cutoff_date = pd.to_datetime(cutoff_date, utc=True)

    listening_history = Doc('listening_history')

    def condition(track):
        has_artist = False
        for artist in track.artists:
            if artist['id'] == artist_id:
                has_artist = True

        if not has_artist:
            return False

        if track.added_at > cutoff_date:
            return False

        return True

    bool_array = listening_history.get_df().apply(condition, axis=1)

    removed_tracks = listening_history.get_df().loc[bool_array]
    remaining_tracks = listening_history.get_df().loc[~bool_array]

    # continue here

    return


def write_file_workspace():
    queue = Doc('queue')

    tmp_doc = Doc('tmp', create=True)

    tmp_doc.append(queue, prompt=False)
    tmp_doc.write()

    return


def lambda_tmp(t):
    return t['name'].endswith(' - Mixed')


def artist_discog_prep(artist_name):
    tracks = get_artist_discography(artist_name)

    doc = Doc('discog1', create=True)
    doc.append(tracks, prompt=False)
    doc.write()
    return

def artist_discog_workspace_3(artist_name):
    latest = 10
    popular = 10

    discogs = Doc('discog1')

    print(f'Found {len(discogs)} tracks')

    listening_history = Doc('listening_history')
    queue = Doc('queue')

    discogs.remove(listening_history, deep=True, prompt=False)
    discogs.remove(queue, deep=True)

    print(f'Left after removing listening history and queue: {len(discogs)} tracks')

    if latest > 0:
        discogs.sort('added_at', ascending=False)

        latest_tracks = discogs.get_df()[:latest]

        discogs.remove(latest_tracks, prompt=False)

        print('Latest tracks:')
        pretty_print_tracks(latest_tracks, indent=' '*4, enum=True, extra_attribs='added_at')

        queue.append(latest_tracks)

    if popular > 0:
        discogs.sort('popularity', ascending=False)

        most_popular_tracks = discogs.get_df()[:popular]

        print('Most popular tracks:')
        pretty_print_tracks(most_popular_tracks, indent=' '*4, enum=True, extra_attribs='popularity')

        queue.append(most_popular_tracks)

    # queue.write()

    return


def main():
    # p = SpotifyPlaylist('L2 queue')
    # df = p.get_df()

    artist_name = 'Ivan Baffa'

    # artist_discog_prep(artist_name)

    # artist_discog_workspace_3(artist_name)

    sample_artist_to_queue(artist_name)

    return

if __name__ == '__main__':
    main()
    sys.exit(0)

