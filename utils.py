
import sys
import pandas as pd

def get_attrib_or_fail(series, attrib_possible_names):
    for attrib in attrib_possible_names:
        if attrib in series:
            return series[attrib]
    raise Exception('None of the attributes %s are present in series %s' % (attrib_possible_names, series))


def format_track(track):
    artists = get_attrib_or_fail(track, ['artist_names', 'artists', 'Artists'])
    title = get_attrib_or_fail(track, ['Title', 'name'])

    if isinstance(artists, list):
        artists = ', '.join(artists)

    return '%s \u2013 %s' % (artists, title)

def pretty_print_tracks(tracks, indent='', enum=False):
    num_tracks = len(tracks)

    if isinstance(tracks, pd.DataFrame):
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
