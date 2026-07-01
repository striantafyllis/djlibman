
import sys

from spyroslib.general_utils import *

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
