
"""
Utility functions dependent only on Python libraries
"""
import inspect
import re
import os
import os.path
import shutil
import sys
import difflib

import numpy as np
import pandas as pd


def to_boolean(s):
    if s is None or isinstance(s, bool):
        return s
    if s.upper() in ['T', 'TRUE']:
        return True
    if s.upper() in ['F', 'FALSE']:
        return False
    raise ValueError()


def project(table, projection):
    if isinstance(table, list):
        return [project(row, projection) for row in table]

    if isinstance(table, dict):
        row = table

        result_dict = {}

        if isinstance(projection, dict):
            for key, value_func in projection.items():
                if value_func is None:
                    value = row[key]
                elif isinstance(value_func, str):
                    value = row[value_func]
                elif inspect.isclass(value_func):
                    value = value_func(row[key])
                elif isinstance(value_func, tuple) and len(value_func) == 2:
                    type_conv, row_key = value_func
                    value = type_conv(row[row_key])
                elif callable(value_func):
                    value = value_func(row)
                elif isinstance(value_func, dict) or isinstance(value_func, list):
                    value = project(row[key], value_func)
                else:
                    value = value_func

                result_dict[key] = value

        elif isinstance(projection, list):
            result_dict = {key: row[key] for key in projection}
        else:
            raise ValueError("Invalid project argument: '%s'" % projection)

        return result_dict

    raise ValueError('Invalid project value type: %s' % type(table))


def infer_type(series):
    """Given a Pandas series of strings, this infers a type that all elements can be converted to:
       integer, float, or timestamp. If such a type is found, a new series of the new type is returned."""

    new_series = None
    converter = None

    _converters = [
        to_boolean,
        np.int64,
        np.float64,
        lambda s: pd.to_datetime(s, utc=True)
    ]

    for i in range(len(series)):
        el = series.iloc[i]

        if el is None:
            continue

        if converter is None:
            for conv in _converters:
                try:
                    new_el = conv(el)
                    converter = conv
                    new_series = pd.Series(index=series.index, dtype=object)
                    new_series.iloc[i] = new_el
                    break
                except ValueError:
                    continue

                if converter is None:
                    # failed to find a type
                    return series
        else:
            try:
                new_series.iloc[i] = converter(el)
            except ValueError:
                if converter == np.int64:
                    converter = np.float64
                    try:
                        new_series.iloc[i] = converter(el)
                    except ValueError:
                        # failed to find a type
                        return series
                else:
                    # failed to find a type
                    return series

    if converter is None:
        return series

    new_series = new_series.convert_dtypes()

    return new_series

def infer_types(df):
    df = df.apply(lambda column: infer_type(column), axis=0)

    return df


def duplicate_positions(iterable):
    already_seen_values = set()

    positions = []

    for i, value in enumerate(iterable):
        if value in already_seen_values:
            positions.append(i)
        else:
            already_seen_values.add(value)

    assert len(positions) == len(iterable) - len(already_seen_values)

    return positions


def dataframe_duplicate_index_labels(df):
    """Returns the positions of duplicate index labels in a dataframe.
    I'm surprised that Pandas doesn't already offer this."""

    unique_idx = df.index.unique()

    if len(df.index) == len(unique_idx):
        return []

    return duplicate_positions(df.index)


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

def dataframe_filter(df, filter):
    bool_array = df.apply(filter, axis=1)
    return df.loc[bool_array]

def get_attrib_or_fail(series, attrib_possible_names):
    for attrib in attrib_possible_names:
        if attrib in series:
            return series[attrib]
    raise Exception('None of the attributes %s are present in series %s' % (attrib_possible_names, series))


def delete_backups(path):
    filename = os.path.basename(path)
    directory = os.path.dirname(path)

    potential_backups = os.listdir(directory)

    backups = [
        backup for backup in potential_backups
        if backup.startswith(filename) and re.fullmatch(r'\.bak(\.[0-9]+)?', backup[len(filename):])
    ]

    if len(backups) == 0:
        return

    for backup in backups:
        os.unlink(os.path.join(directory, backup))

    return

def _backup_name(path, backup_num):
    if backup_num == 0:
        return path + '.bak'
    else:
        return path + '.bak' + '.%d' % backup_num

def _move_backup(path, backup_num, max_backups):
    this_backup = _backup_name(path, backup_num)

    if not os.path.exists(this_backup):
        return

    if backup_num >= max_backups-1:
        # just delete it
        os.unlink(this_backup)
    else:
        # rename it to the next backup
        _move_backup(path, backup_num+1, max_backups)
        next_backup = _backup_name(path, backup_num+1)
        os.rename(this_backup, next_backup)

    return

def back_up_file(path, max_backups):
    if max_backups <= 0:
        return

    if not os.path.exists(path):
        return

    _move_backup(path, 0, max_backups)

    backup = _backup_name(path, 0)
    # os.rename(self._path, backup)
    shutil.copyfile(path, backup)
    return

def format_track(track, id=True, extra_attribs=[]):
    if isinstance(extra_attribs, str):
        extra_attribs = [extra_attribs]

    s = ''

    if id:
        for id_field in ['rekordbox_id', 'spotify_id']:
            if id_field in track:
                s += f'{track[id_field]}: '
                break

    if 'artist_names' in track:
        # Spotify-style track
        artists = ', '.join(track['artist_names'].split('|'))
    elif 'Artists' in track:
        artists = track['Artists']

    title = get_attrib_or_fail(track, ['Title', 'name'])

    s += f'{artists} \u2013 {title}'

    for extra_attrib in extra_attribs:
        s += f' {extra_attrib}={track[extra_attrib]}'

    return s


def format_track_for_search(track):
    """Creates a search string that's more likely to generate matches out of a
    track's artists and title."""

    if isinstance(track, str):
        string = track.lower()
    else:
        string = get_attrib_or_fail(track, ['Title', 'name'])
        string = string.lower()

    # Remove some things that are usually in Rekordbox but not in Spotify
    string = re.sub(r'(feat\.|featuring)', '', string, flags=re.IGNORECASE)
    string = re.sub(r'original mix', '', string, flags=re.IGNORECASE)

    if not isinstance(track, str):
        if 'artist_names' in track:
            artists_list = [artist.lower() for artist in track['artist_names'].split('|')]

            # Work around a big difference between Spotify and everyone else:
            # In Spotify, remix artists are in the artists' list; in other services they aren't
            filtered_artists_list = [artist for artist in artists_list if artist not in string]

            artists = ' '.join(filtered_artists_list)
        elif 'Artists' in track:
            artists = track['Artists'].replace(',', ' ').lower()
        else:
            raise Exception("None of the attributes %s are present in series %s" % (
                ['artists', 'Artists'],
                track
            ))

        # sort the words in the artist string; this is necessary because Spotify and Rekordbox
        # often list artists in different order.
        # This will also mix up first and last names of the same artist; I don't see a way to avoid this
        artist_words = artists.split()
        artist_words.sort()

        string = ' '.join(artist_words) + ' ' + string

    # replace sequences of non-word characters with a single space
    string = re.sub(r'\W+', ' ', string)

    # replace multiple spaces with single space
    string = ' '.join(string.split())

    # get rid of capitalization problems
    string = string.lower()

    return string


def pretty_print_tracks(tracks, indent='', enum=False, ids=True, extra_attribs=[]):
    num_tracks = len(tracks)
    if num_tracks == 0:
        return

    if hasattr(tracks, 'get_df'):
        tracks = tracks.get_df()

    if isinstance(tracks, pd.DataFrame):
        if tracks.empty:
            return
        tracks = tracks.iloc

    for i in range(num_tracks):
        sys.stdout.write(indent)
        if enum:
            sys.stdout.write(f'{i+1}. ')

        sys.stdout.write(format_track(tracks[i], id=ids, extra_attribs=extra_attribs) + '\n')

    sys.stdout.flush()

    return

def pretty_print_albums(albums, indent='', enum=False):
    for i, album in enumerate(albums):
        sys.stdout.write(indent)
        if enum:
            sys.stdout.write(f'{i+1}: ')
        sys.stdout.write(f'{album['id']}: {album['name']} ({album['total_tracks']} tracks)\n')

    sys.stdout.flush()

    return

def pretty_print(data_structure, indent='', level_indent=' '*4):
    """Good for pretty-printing nested maps and lists"""
    if isinstance(data_structure, dict):
        sys.stdout.write('{\n')
        for key, value in data_structure.items():
            sys.stdout.write(indent + level_indent + repr(key) + ': ')
            pretty_print(value, indent=indent+level_indent, level_indent=level_indent)
        sys.stdout.write(indent + '}\n')
    elif isinstance(data_structure, list):
        sys.stdout.write('[\n')
        for value in data_structure:
            sys.stdout.write(indent + level_indent)
            pretty_print(value, indent=indent+level_indent, level_indent=level_indent)
        sys.stdout.write(indent + ']\n')
    else:
        sys.stdout.write(repr(data_structure) + '\n')

    sys.stdout.flush()
    return



def get_user_choice(prompt: str, options: list[str] = ['yes', 'no'], batch_mode: bool = False, exit_option=True):
    """Allows the user to choose among a number of options by typing any unambiguous prefix
    (usually the first letter) of an option"""
    assert len(options) > 0

    if batch_mode:
        return options[0]

    if exit_option:
        if 'exit' in options:
            raise Exception("exit_option=True but 'exit' already in options")
        options = options + ['exit']

    while True:
        sys.stdout.write(prompt + ' (' + '/'.join(options) + ') > ')
        sys.stdout.flush()

        reply = sys.stdin.readline().strip()

        possible_options = [option for option in options if option.upper().startswith(reply.upper())]

        if len(possible_options) == 1:
            choice = possible_options[0]
            if exit_option and choice == 'exit':
                raise KeyboardInterrupt()
            return choice
        elif len(possible_options) == 0:
            sys.stdout.write('Reply not recognized; try again.')
        else:
            sys.stdout.write('Reply is ambiguous; try again.')

def fuzzy_one_to_one_mapping(sequences1, sequences2, cutoff_ratio=0.6):
    """Creates a one-to-one mapping between two string lists using fuzzy text matching.
    Only pairings with a match ratio of at least cutoff_ratio are considered.
    It is assumed that both sequences are relatively short and contain relatively short strings.
    Returns:
        {
           pairs: [ { index1: <index into sequences1>,
                      index2: <index into sequences2>,
                      ratio: <match ratio>
                      },
                      ...
           unmatched_indices1: [ indices into sequences1 ... ],
           unmatched_indices2: [ indices into sequences2 ...]
        }
    """

    # using dict instead of set to preserve the order
    unmatched_indices1 = { index: None for index in range(len(sequences1))}
    unmatched_indices2 = { index: None for index in range(len(sequences2))}

    sequence_matcher = difflib.SequenceMatcher()

    all_pairs = []

    for index1 in range(len(sequences1)):
        for index2 in range(len(sequences2)):
            sequence_matcher.set_seqs(sequences1[index1], sequences2[index2])
            ratio = sequence_matcher.ratio()
            if ratio < cutoff_ratio:
                continue

            all_pairs.append( {
                'index1': index1,
                'index2': index2,
                'ratio': ratio
            })

    all_pairs.sort(key=lambda x: x['ratio'], reverse=True)

    result = []

    for pair in all_pairs:
        if len(unmatched_indices1) == 0:
            break
        if len(unmatched_indices2) == 0:
            break

        if pair['index1'] not in unmatched_indices1:
            continue
        if pair['index2'] not in unmatched_indices2:
            continue

        del unmatched_indices1[pair['index1']]
        del unmatched_indices2[pair['index2']]

        result.append(pair)

    return {
        'pairs': result,
        'unmatched_indices1': unmatched_indices1,
        'unmatched_indices2': unmatched_indices2
    }



def list_condition(condition, mode='or'):
    def _apply_condition_to_list(lst, condition, mode='or'):
        for el in lst:
            if condition(el):
                if mode == 'or':
                    return True
            else:
                if mode == 'and':
                    return False

        # empty list case
        return mode == 'and'

    if mode not in ['and', 'or']:
        raise Exception("Invalid mode '%s'" % mode)

    return lambda lst: _apply_condition_to_list(lst, condition, mode=mode)


def artist_name_condition(name_condition, mode='or'):
    if mode not in ['and', 'or']:
        raise Exception("Invalid mode '%s'" % mode)

    return lambda row: list_condition(lambda el: name_condition(el['name']))(row.artists)


def artist_stats(tracks, count_cutoff=10):
    artists_to_counts = {}

    def _handle_artists(artists):
        for artist in artists:
            artists_to_counts[artist['name']] = artists_to_counts.get(artist['name'], 0) + 1

    tracks.artists.apply(_handle_artists)

    artist_names = list(artists_to_counts)
    artist_names.sort(key=lambda x: artists_to_counts[x], reverse=True)

    for artist_name in artist_names:
        count = artists_to_counts[artist_name]
        if count < count_cutoff:
            break
        print('%s: %d' % (artist_name, count))

    return
