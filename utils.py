
import sys
import re
import difflib
import pandas as pd

from internal_utils import *

def format_track(track):
    s = ''

    for id_field in ['TrackID', 'id']:
        if id_field in track:
            s += '%s: ' % track[id_field]
            break

    if 'artists' in track:
        # Spotify-style track; artists is a list of dict
        artists = ', '.join(artist['name'] for artist in track['artists'])
    elif 'Artists' in track:
        artists = track['Artists']

    title = get_attrib_or_fail(track, ['Title', 'name'])

    return s + '%s \u2013 %s' % (artists, title)

def format_track_for_search(track):
    """Creates a search string that's more likely to generate matches out of a
    track's artists and title."""

    if 'artists' in track:
        artists = ' '.join(artist['name'] for artist in track['artists'])
    elif 'Artists' in track:
        artists = track['Artists']
    else:
        raise Exception("None of the attributes %s are present in series %s" % (
            ['artists', 'Artists'],
            track
        ))

    title = get_attrib_or_fail(track, ['Title', 'name'])

    # Remove some things that are usually in Rekordbox but not in Spotify
    title = re.sub(r'(feat\.|featuring)', '', title, flags=re.IGNORECASE)
    title = re.sub(r'original mix', '', title, flags=re.IGNORECASE)

    string = artists + ' ' + title

    # replace sequences of non-word characters with a single space
    string = re.sub(r'\W+', ' ', string)

    # get rid of capitalization problems
    string = string.lower()

    return string




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
