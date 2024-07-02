
import sys
import pandas as pd

def get_attrib_or_fail(series, attrib_possible_names):
    for attrib in attrib_possible_names:
        if attrib in series:
            return series[attrib]
    raise Exception('None of the attributes %s are present in series %s' % (attrib_possible_names, series))


def format_track(track):
    if 'id' in track:
        s = '%s: ' % track['id']
    else:
        s = ''

    if 'artists' in track:
        # Spotify-style track; artists is a list of dict
        artists = ', '.join(artist['name'] for artist in track['artists'])
    else:
        artists = get_attrib_or_fail(track, ['artist_names', 'Artists'])
        if isinstance(artists, list):
            artists = ', '.join(artists)

    title = get_attrib_or_fail(track, ['Title', 'name'])

    return s + '%s \u2013 %s' % (artists, title)

def pretty_print_tracks(tracks, indent='', enum=False):
    num_tracks = len(tracks)
    if num_tracks == 0:
        return

    if isinstance(tracks, pd.DataFrame):
        if tracks.empty:
            return
        tracks = tracks.iloc

    for i in range(num_tracks):
        sys.stdout.write(indent)
        if enumerate:
            sys.stdout.write('%d: ' % (i+1))

        sys.stdout.write('%s\n' % format_track(tracks[i]))

    return

def get_user_choice(prompt: str, options: list[str] = ['yes', 'no'], batch_mode: bool = False):
    """Allows the user to choose among a number of options by typing any unambiguous prefix
    (usually the first letter) of an option"""
    assert len(options) > 0

    if batch_mode:
        return options[0]

    while True:
        sys.stdout.write(prompt + ' (' + '/'.join(options) + ') > ')
        sys.stdout.flush()

        reply = sys.stdin.readline().strip()

        possible_options = [option for option in options if option.upper().startswith(reply.upper())]

        if len(possible_options) == 1:
            return possible_options[0]
        elif len(possible_options) == 0:
            sys.stdout.write('Reply not recognized; try again.')
        else:
            sys.stdout.write('Reply is ambiguous; try again.')


def dataframe_duplicate_index_labels(df):
    """Returns the positions of duplicate index labels in a dataframe.
    I'm surprised that Pandas doesn't already offer this."""

    unique_idx = df.index.unique()

    if len(df.index) == len(unique_idx):
        return []

    already_seen_labels = set()

    positions = []

    for i, label in enumerate(df.index):
        if label in already_seen_labels:
            positions.append(i)
        else:
            already_seen_labels.add(label)

    assert len(positions) == len(df.index) - len(unique_idx)

    return positions

def dataframe_drop_rows_at_positions(df, positions):
    """Returns a new dataframe without the rows indicated by the positions.
    I'm surprised pandas doesn't already offer this."""

    new_df_index = [(i not in positions) for i in range(len(df))]

    new_df = df.loc[new_df_index]

    return new_df

def dataframe_ensure_unique_index(df):
    """Makes sure that all dataframe index entries are unique by removing rows that
    have the same index label as a previous row. I'm surprised pandas doesn't already offer this."""

    pos = dataframe_duplicate_index_labels(df)
    return dataframe_drop_rows_at_positions(df, pos)

