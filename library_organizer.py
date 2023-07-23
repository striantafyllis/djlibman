#!/usr/bin/env python

import sys
import argparse

import rekordbox

class RekordboxState:
    def __init__(self,
                 collection: rekordbox.Collection,
                 playlists: dict[str, rekordbox.Playlist]):
        self.collection = collection
        self.playlists = playlists

        self.main_library = playlists.get('Main Library')
        self.second_look = playlists.get('second look')
        self.backlog = playlists.get('backlog')
        self.other = playlists.get('other / non-house')
        return

def read_rekordbox(rekordbox_xml):
    collection, playlists = rekordbox.parse_library(rekordbox_xml)

    return RekordboxState(collection, playlists)

def rekordbox_stats(rekordbox_state: RekordboxState):
    print('Rekordbox:')
    print('  Total tracks: %d' % len(rekordbox_state.collection.tracks_by_id))

    if rekordbox_state.main_library is None:
        sys.stderr.write("  WARNING: No 'Main Library' playlist\n")
    else:
        print('  Main Library tracks: %d' % len(rekordbox_state.main_library.tracks))

    if rekordbox_state.second_look is None:
        sys.stderr.write("  WARNING: No 'second look' playlist\n")
    else:
        print('  Second look tracks: %d' % len(rekordbox_state.second_look.tracks))

    if rekordbox_state.backlog is None:
        sys.stderr.write("  WARNING: No 'backlog' playlist")
    else:
        print('  Backlog tracks: %d' % len(rekordbox_state.backlog.tracks))

    if rekordbox_state.other is None:
        sys.stderr.write("  WARNING: No 'other / non-house' playlist")
    else:
        print('  Other/non-house tracks: %d' % len(rekordbox_state.other.tracks))

def rekordbox_sanity_checks(rekordbox_state):
    # check that each track is in at most one of the 4 main lists
    for track in rekordbox_state.collection.all_tracks_by_id():
        containing_playlists = []
        if track in rekordbox_state.main_library:
            containing_playlists.append('Main Library')
        if track in rekordbox_state.second_look:
            containing_playlists.append('second look')
        if track in rekordbox_state.backlog:
            containing_playlists.append('backlog')
        if track in rekordbox_state.other:
            containing_playlists.append('other / non-house')

        if len(containing_playlists) == 0:
            sys.stderr.write('Track is not in any top-level playlist: %s\n' % track)
        elif len(containing_playlists) > 1:
            sys.stderr.write('Track is in multiple top-level playists %s: %s\n' % (containing_playlists, track))

    return


def main():
    parser = argparse.ArgumentParser(
        prog='library_organizer.py',
        description='Organizes my music library between Rekordbox, Spotify and YouTube with help from some Google docs'
    )
    parser.add_argument('--rekordbox_xml', default=rekordbox.default_rekordbox_xml)

    args = parser.parse_args()

    rekordbox_state = read_rekordbox(args.rekordbox_xml)

    rekordbox_stats(rekordbox_state)
    rekordbox_sanity_checks(rekordbox_state)

    return 0




if __name__ == '__main__':
    exit_value = main()
    sys.exit(exit_value)
