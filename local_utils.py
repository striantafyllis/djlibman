
import sys
import re
import difflib
import inspect

import pandas as pd
import unidecode

from spyroslib.general_utils import *


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
            raise ValueError(f"Invalid project argument: '{projection}'")

        return result_dict

    raise ValueError(f'Invalid project value type: {type(table)}')


def get_attrib_or_fail(series, attrib_possible_names):
    for attrib in attrib_possible_names:
        if attrib in series and not pd.isna(series[attrib]):
            return series[attrib]
    raise Exception(f'None of the attributes {attrib_possible_names} are '
                    f'present in series {series}')


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

def get_track_signature(track):
    """Returns a value that should uniquely identify the track in most contexts;
       the value is a tuple contains the artist names and the track title"""
    name = track['name']
    artist_names = track['artist_names'].upper().split('|')

    # get rid of parenthesized combinations of uppercase letters and numbers - these are usually label codes
    name = re.sub(r'(\[|\()[A-Z]{3,100} ?[0-9]+(\]|\))', '', name)

    name = name.upper()

    # get read of "featuring ...", "feat. " etc.
    name = re.sub(r'FEAT(\.|URING) .*', '', name)

    for s in ['(', ')', '[', ']', '-', ' AND ', ' X ', 'EXTENDED', 'ORIGINAL', 'REMIX', 'MIXED', 'MIX', 'RADIO', 'EDIT']:
        name = name.replace(s, '')

    # get rid of whitespace differences
    name = ' '.join(name.split())

    artist_names.sort()

    return tuple(artist_names + [name])


def string_to_sorted_tokens(s, asciify=False):
    words = s.split()

    if asciify:
        words = [unidecode.unidecode(w) for w in words]

    return ' '.join(sorted(set(words)))


def fuzzy_one_to_one_mapping(sequences1, sequences2, cutoff_ratio=0.6,
                             tokenwise=False,
                             asciify=False):
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

    if tokenwise:
        sequences1 = [string_to_sorted_tokens(s, asciify=asciify) for s in sequences1]
        sequences2 = [string_to_sorted_tokens(s, asciify=asciify) for s in sequences2]

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
