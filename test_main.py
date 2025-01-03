
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


def read_file(filename):
    fh = open(filename)

    lines = []

    for line in fh:
        line = line.strip()
        if line.startswith('#'):
            continue

        lines.append(line)

    fh.close()

    return lines


def main():
    artists = read_file('./sample_artists.txt')

    for artist in artists:
        sample_artist_to_queue(artist)

    # replenish_spotify_queue()

    return

if __name__ == '__main__':
    main()
    sys.exit(0)

