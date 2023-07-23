#!/usr/bin/env python

import sys
import argparse

import rekordbox
import google_sheet

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
    num_errors = 0
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
            num_errors += 1
        elif len(containing_playlists) > 1:
            sys.stderr.write('Track is in multiple top-level playists %s: %s\n' % (containing_playlists, track))
            num_errors += 1

    if num_errors > 0:
        sys.stderr.write('** Fix the above %d errors manually! ***\n' % num_errors)
        sys.exit(1)

    return

def sheet_stats(sheet: google_sheet.Sheet):
    print("Google sheet ID='%s' page='%s': %d entries" % (
        sheet.id,
        sheet.page,
        len(sheet.tracks)
    ))
    return


def cross_reference_rekordbox_to_google_sheet(
        rekordbox_state: RekordboxState,
        sheet: google_sheet.Sheet):

    num_errors = 0
    num_missing_ids = 0

    for track_info in sheet.tracks:
        if track_info.id is not None:
            track = rekordbox_state.collection.tracks_by_id.get(track_info.id)

            if track is None:
                sys.stderr.write('Sheet row %d: track ID %d not found in Rekordbox\n' % (
                    track_info.row_num, track_info.id))
                num_errors += 1
        else:
            # try to match by artists and title
            num_missing_ids += 1
            artists = frozenset(track_info.artists)

            track = rekordbox_state.collection.tracks_by_artists_and_name[artists].get(track_info.title)

            if track is None:
                sys.stderr.write("Sheet row %d: track artists '%s' title '%s' not found in Rekordbox\n" % (
                    track_info.row_num, track_info.artists, track_info.title))
                num_errors += 1
            else:
                track_info.id = track.id
                track_info.dirty_fields.append('id')

        if track is not None:
            if track.track_info is not None:
                sys.stderr.write("Rekordbox track artists '%s' title '%s' is associated with 2 sheet entries: rows %d and %d" % (
                    track.artists,
                    track.title,
                    track.track_info.row_num,
                    track_info.row_num
                ))
                num_errors += 1
            else:
                track_info.track = track
                track.track_info = track_info

    if num_errors > 0:
        sys.stderr.write('*** Fix the above %d errors manually! ***\n' % num_errors)
        sys.exit(1)

    if num_missing_ids > 0:
        sys.stderr.write('%d missing track IDs in Google sheet!\n' % num_missing_ids)
        sys.stderr.write('Write missing IDs? (y/n) >')
        sys.stderr.flush()

        reply = sys.stdin.readline().strip()

        if reply.upper() == 'Y' or reply.upper() == 'YES':
            updated_cells = sheet.write_back()
            sys.stderr.write('*** Wrote %d missing IDs; verify manually! ***\n' % updated_cells)
            sys.exit(1)
        else:
            sys.stderr.write('*** Fix missing IDs first! ***\n')
            sys.exit(1)

    return


def main():
    parser = argparse.ArgumentParser(
        prog='library_organizer.py',
        description='Organizes my music library between Rekordbox, Spotify and YouTube with help from some Google docs'
    )
    parser.add_argument('--rekordbox_xml', default=rekordbox.default_rekordbox_xml)
    parser.add_argument('--google_sheet_id', default=google_sheet.DEFAULT_SPREADSHEET_ID)
    parser.add_argument('--google_sheet_page', default=google_sheet.DEFAULT_SPREADSHEET_PAGE)

    args = parser.parse_args()

    rekordbox_state = read_rekordbox(args.rekordbox_xml)

    sheet = google_sheet.parse_sheet(args.google_sheet_id, args.google_sheet_page)

    rekordbox_stats(rekordbox_state)
    rekordbox_sanity_checks(rekordbox_state)

    sheet_stats(sheet)

    cross_reference_rekordbox_to_google_sheet(rekordbox_state, sheet)

    return 0




if __name__ == '__main__':
    exit_value = main()
    sys.exit(exit_value)
