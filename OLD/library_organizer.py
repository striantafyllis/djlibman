#!/usr/bin/env python
"""
Creates a single library out of Rekordbox, the Google sheet, and the streaming services.
Handles synchronization between all three.
"""

import urllib.parse

from data_model import *
import rekordbox
import google_sheet
from streaming_service import StreamingService
from spotify_service import SpotifyService
from youtube_service import YouTubeService
from utils import *


class RekordboxState:
    collection: Library
    playlists: dict[str, Playlist]
    main_library: Playlist
    second_look: Playlist
    backlog: Playlist
    other: Playlist

    def __init__(self,
                 collection: Library,
                 playlists: dict[str, Playlist]):
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
    print('  Total tracks: %d' % len(rekordbox_state.collection))

    if rekordbox_state.main_library is None:
        sys.stderr.write("  WARNING: No 'Main Library' playlist\n")
    else:
        print('  Main Library tracks: %d' % len(rekordbox_state.main_library))

    if rekordbox_state.second_look is None:
        sys.stderr.write("  WARNING: No 'second look' playlist\n")
    else:
        print('  Second look tracks: %d' % len(rekordbox_state.second_look))

    if rekordbox_state.backlog is None:
        sys.stderr.write("  WARNING: No 'backlog' playlist")
    else:
        print('  Backlog tracks: %d' % len(rekordbox_state.backlog))

    if rekordbox_state.other is None:
        sys.stderr.write("  WARNING: No 'other / non-house' playlist")
    else:
        print('  Other/non-house tracks: %d' % len(rekordbox_state.other))

def rekordbox_sanity_checks(rekordbox_state):
    # check that each track is in at most one of the 4 main lists
    num_errors = 0
    for track in rekordbox_state.collection:
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
        sys.stderr.write('*** Fix the above %d errors manually! ***\n' % num_errors)
        sys.exit(1)

    return

def sheet_stats(sheet: google_sheet.GoogleSheet):
    print("Google sheet ID='%s' page='%s': %d entries" % (
        sheet.id,
        sheet.page,
        len(sheet)
    ))
    return


def cross_reference_rekordbox_to_google_sheet(
        batch_mode: bool,
        rekordbox_state: RekordboxState,
        sheet: google_sheet.GoogleSheet):

    num_errors = 0
    num_missing_ids = 0

    for sheet_track in sheet:
        if sheet_track['Rekordbox ID'] is not None:
            rekordbox_track = rekordbox_state.collection.get_track_by_id(sheet_track['Rekordbox ID'])

            if rekordbox_track is None:
                sys.stderr.write('Sheet row %d: track ID %d not found in Rekordbox\n' % (
                    sheet_track.id, sheet_track['Rekordbox ID']))
                num_errors += 1
        else:
            # try to match by artists and title
            num_missing_ids += 1

            rekordbox_track = rekordbox_state.collection.get_track_by_artists_and_name(sheet_track.artists, sheet_track.title)

            if rekordbox_track is None:
                sys.stderr.write("Sheet row %d: track artists '%s' title '%s' not found in Rekordbox\n" % (
                    sheet_track.id, sheet_track.artists, sheet_track.title))
                num_errors += 1
            else:
                sheet_track['Rekordbox ID'] = rekordbox_track.rekordbox_id

    if num_errors > 0:
        sys.stderr.write('*** Fix the above %d errors manually! ***\n' % num_errors)
        sys.exit(1)

    if num_missing_ids > 0:
        print('%d missing track IDs in Google sheet!' % num_missing_ids)
        reply = get_user_choice(batch_mode, 'Write missing IDs?')

        if reply == 'yes':
            updated_cells = sheet.write_back()
            print('*** Wrote %d missing IDs; verify manually! ***' % updated_cells)
            # sys.exit(1)
        else:
            sys.stderr.write('*** Fix missing IDs first! ***\n')
            sys.exit(1)

    return


def reconcile_sheet_with_rekordbox(
        batch_mode: bool,
        sheet: google_sheet.GoogleSheet,
        rekordbox_state: RekordboxState):

    num_errors = 0
    num_tracks_missing_from_google_sheet = 0

    for sheet_track in sheet:
        # previous sanity checks have already checked that the Rekordbox track exists...
        rekordbox_track = rekordbox_state.collection.get_track_by_id(sheet_track['Rekordbox ID'])

        if rekordbox_track not in rekordbox_state.main_library:
            sys.stderr.write('%s is in the Google sheet but not in Rekordbox Main Library\n' % sheet_track)
            num_errors += 1

    if num_errors > 0:
        sys.stderr.write('*** Fix the above %d errors manually! ***\n' % num_errors)
        sys.exit(1)

    for rekordbox_track in rekordbox_state.main_library:
        sheet_track = sheet.get_track_by_foreign_id('Rekordbox', rekordbox_track.id)

        if sheet_track is None:
            print('%s is in Rekordbox Main Library but not in the Google sheet; adding.' % rekordbox_track)
            sheet_track = google_sheet.SheetTrack(
                sheet,
                sheet.next_row(),
                rekordbox_track.artists,
                rekordbox_track.title)

            sheet.append(sheet_track)

            for attribute in sheet.header:
                if attribute in rekordbox_track:
                    sheet_track[attribute] = rekordbox_track[attribute]

            num_tracks_missing_from_google_sheet += 1

    if num_tracks_missing_from_google_sheet > 0:
        print('%d tracks missing from Google sheet!' % num_tracks_missing_from_google_sheet)
        reply = get_user_choice(batch_mode, 'Add missing tracks?')

        if reply == 'yes':
            updated_cells = sheet.write_back()
            print('*** Added %d missing tracks (%d cells); verify manually! ***' % (
                num_tracks_missing_from_google_sheet, updated_cells))
        else:
            sys.stderr.write('*** Fix missing tracks first! ***\n')
            sys.exit(1)

    num_mismatched_fields = 0

    # Make sure all common attributes have the same value between Google sheet and Rekordbox;
    # if not, write the corrected values in the Google sheet.
    # Then merge all Rekordbox attributes into the Google sheet tracks in memory;
    # this allows us to query with a mix of sheet and Rekordbox-only attributes later.
    for sheet_track in sheet:
        rekordbox_track = rekordbox_state.collection.get_track_by_id(sheet_track['Rekordbox ID'])

        for attribute, rekordbox_value in rekordbox_track.items():
            if attribute not in sheet_track:
                sheet_track[attribute] = rekordbox_value
            elif sheet_track[attribute] != rekordbox_value:
                print("%s: attribute %s has value '%s' in Google sheet but '%s' in Rekordbox" % (
                    sheet_track, attribute, sheet_track[attribute], rekordbox_value
                ))
                num_mismatched_fields += 1

                sheet_track[attribute] = rekordbox_value

    if num_mismatched_fields > 0:
        print('%d mismatching fields in Google sheet!' % num_mismatched_fields)
        reply = get_user_choice(batch_mode, 'Correct mismatched fields?')

        if reply == 'yes':
            updated_cells = sheet.write_back()
            print('*** Corrected %d mismatched fields; verify manually! ***' % updated_cells)
            # sys.exit(1)
        else:
            sys.stderr.write('*** Fix mismatched fields first! ***\n')
            sys.exit(1)


def write_m3u_playlist(
        rekordbox_state: RekordboxState,
        playlist_filename: str,
        tracklist: list[Track]):
    if not playlist_filename.endswith('.m3u8'):
        playlist_filename += '.m3u8'

    playlist_file = open(playlist_filename, 'w')

    playlist_file.write('#EXTM3U\n')

    for track in tracklist:
        if not isinstance(track, rekordbox.RekordboxTrack):
            track = rekordbox_state.collection.get_track_by_id(track['Rekordbox ID'])
            if track is None:
                print('WARNING: Skipping track %s; no Rekordbox ID' % track)
                continue
        playlist_file.write('#EXTINF:%d,%s \u2013 %s\n' % (
            track['Duration'],
            track['Artists'],
            track.title
        ))

        location = track['Location']
        if location.startswith('file://localhost'):
            location = urllib.parse.unquote(location[16:])

        playlist_file.write(location + '\n')

    playlist_file.close()

    print("Wrote file '%s' (%d tracks)" % (playlist_filename, len(tracklist)))



youtube_service: YouTubeService = None
spotify_service: SpotifyService = None

def get_streaming_service_by_name(service_name: str) -> StreamingService:
    global youtube_service
    global spotify_service
    if service_name.upper() == 'YOUTUBE':
        if youtube_service is None:
            youtube_service = YouTubeService()
        service = youtube_service
    elif service_name.upper() == 'SPOTIFY':
        if spotify_service is None:
            spotify_service = SpotifyService()
        service = spotify_service
    else:
        raise Exception('Unimplemented streaming service %s' % service_name)

    return service


def main():
    print('Nothing doing')

    return 0


if __name__ == '__main__':
    exit_value = main()
    sys.exit(exit_value)
