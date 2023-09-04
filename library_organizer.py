#!/usr/bin/env python
"""
Creates a single library out of Rekordbox, the Google sheet, and the streaming services.
Handles synchronization between all three.
"""


from data_model import *
import rekordbox
# import google_sheet
# from streaming_service import StreamingService
# from spotify_service import SpotifyService
# from youtube_service import YouTubeService
from utils import *


class RekordboxState:
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
    print('  Total tracks: %d' % len(rekordbox_state.collection.tracks_by_rekordbox_id))

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
        sys.stderr.write('*** Fix the above %d errors manually! ***\n' % num_errors)
        sys.exit(1)

    return

# def sheet_stats(sheet: google_sheet.Sheet):
#     print("Google sheet ID='%s' page='%s': %d entries" % (
#         sheet.id,
#         sheet.page,
#         len(sheet.tracks)
#     ))
#     return
#
#
# def cross_reference_rekordbox_to_google_sheet(
#         rekordbox_state: RekordboxState,
#         sheet: google_sheet.Sheet):
#
#     num_errors = 0
#     num_missing_ids = 0
#
#     for track_info in sheet.tracks:
#         if track_info.rekordbox_id is not None:
#             track = rekordbox_state.collection.tracks_by_rekordbox_id.get(track_info.rekordbox_id)
#
#             if track is None:
#                 sys.stderr.write('Sheet row %d: track ID %d not found in Rekordbox\n' % (
#                     track_info.row_num, track_info.rekordbox_id))
#                 num_errors += 1
#         else:
#             # try to match by artists and title
#             num_missing_ids += 1
#             artists = frozenset(track_info.artists)
#
#             track = rekordbox_state.collection.tracks_by_artists_and_name[artists].get(track_info.title)
#
#             if track is None:
#                 sys.stderr.write("Sheet row %d: track artists '%s' title '%s' not found in Rekordbox\n" % (
#                     track_info.row_num, track_info.artists, track_info.title))
#                 num_errors += 1
#             else:
#                 track_info.rekordbox_id = track.rekordbox_id
#                 track_info.dirty_fields.add('rekordbox_id')
#
#         if track is not None:
#             if track.track_info is not None:
#                 sys.stderr.write("Rekordbox track artists '%s' title '%s' is associated with 2 sheet entries: rows %d and %d" % (
#                     track.artists,
#                     track.title,
#                     track.track_info.row_num,
#                     track_info.row_num
#                 ))
#                 num_errors += 1
#             else:
#                 track_info.track = track
#                 track.track_info = track_info
#
#     if num_errors > 0:
#         sys.stderr.write('*** Fix the above %d errors manually! ***\n' % num_errors)
#         sys.exit(1)
#
#     if num_missing_ids > 0:
#         print('%d missing track IDs in Google sheet!' % num_missing_ids)
#         reply = get_user_choice('Write missing IDs?')
#
#         if reply == 'yes':
#             updated_cells = sheet.write_back()
#             print('*** Wrote %d missing IDs; verify manually! ***' % updated_cells)
#             # sys.exit(1)
#         else:
#             sys.stderr.write('*** Fix missing IDs first! ***\n')
#             sys.exit(1)
#
#     return
#
#
# def sheet_vs_rekordbox_sanity_checks(
#         sheet: google_sheet.Sheet,
#         rekordbox_state: RekordboxState):
#
#     num_errors = 0
#     num_tracks_missing_from_google_sheet = 0
#
#     for track_info in sheet.tracks:
#         if track_info.track not in rekordbox_state.main_library:
#             sys.stderr.write('%s is in the Google sheet but not in Rekordbox Main Library\n' % track_info)
#             num_errors += 1
#
#     if num_errors > 0:
#         sys.stderr.write('*** Fix the above %d errors manually! ***\n' % num_errors)
#         sys.exit(1)
#
#     for track in rekordbox_state.main_library.tracks:
#         if track.track_info is None:
#             print('%s is in Rekordbox Main Library but not in the Google sheet; adding.' % track)
#             track_info = google_sheet.TrackInfo(sheet)
#             for field in sheet.Track_field_to_col_num.keys():
#                 if field in dir(track):
#                     setattr(track_info, field, getattr(track, field))
#                     track_info.dirty_fields.add(field)
#             track.track_info = track_info
#             num_tracks_missing_from_google_sheet += 1
#
#     if num_tracks_missing_from_google_sheet > 0:
#         print('%d tracks missing from Google sheet!' % num_tracks_missing_from_google_sheet)
#         reply = get_user_choice('Add missing tracks?')
#
#         if reply == 'yes':
#             updated_cells = sheet.write_back()
#             print('*** Added %d missing tracks (%d cells); verify manually! ***' % (
#                 num_tracks_missing_from_google_sheet, updated_cells))
#         else:
#             sys.stderr.write('*** Fix missing tracks first! ***\n')
#             sys.exit(1)
#
#     num_mismatched_fields = 0
#
#     for track_info in sheet.tracks:
#         for field in sheet.Track_field_to_col_num.keys():
#             if field not in dir(track_info.track):
#                 # Spotify/YouTube URIs are in TrackInfo but not in Track...
#                 continue
#             sheet_value = getattr(track_info, field)
#             rekordbox_value = getattr(track_info.track, field)
#             if sheet_value != rekordbox_value:
#                 print("%s: field %s has value '%s' in Google sheet but '%s' in Rekordbox" % (
#                     track_info, field, sheet_value, rekordbox_value
#                 ))
#                 num_mismatched_fields += 1
#
#                 setattr(track_info, field, rekordbox_value)
#                 track_info.dirty_fields.add(field)
#
#     if num_mismatched_fields > 0:
#         print('%d mismatching fields in Google sheet!' % num_mismatched_fields)
#         reply = get_user_choice('Correct mismatched fields?')
#
#         if reply == 'yes':
#             updated_cells = sheet.write_back()
#             print('*** Corrected %d mismatched fields; verify manually! ***' % updated_cells)
#             # sys.exit(1)
#         else:
#             sys.stderr.write('*** Fix mismatched fields first! ***\n')
#             sys.exit(1)
#
#
# def write_m3u_playlist(playlist_filename: str,
#                        tracklist: list[rekordbox.RekordboxTrack]):
#     if not playlist_filename.endswith('.m3u8'):
#         playlist_filename += '.m3u8'
#
#     playlist_file = open(playlist_filename, 'w')
#
#     playlist_file.write('#EXTM3U\n')
#
#     for track in tracklist:
#         playlist_file.write('#EXTINF:%d,%s \u2013 %s\n' % (
#             track.duration,
#             track.artist_orig,
#             track.title
#         ))
#         playlist_file.write(track.location + '\n')
#
#     playlist_file.close()
#
#     print("Wrote file '%s' (%d tracks)" % (playlist_filename, len(tracklist)))
#
#
#
# youtube_service: YouTubeService = None
# spotify_service: SpotifyService = None
#
# def get_streaming_service_by_name(service_name: str) -> StreamingService:
#     global youtube_service
#     global spotify_service
#     if service_name.upper() == 'YOUTUBE':
#         if youtube_service is None:
#             youtube_service = YouTubeService()
#         service = youtube_service
#     elif service_name.upper() == 'SPOTIFY':
#         if spotify_service is None:
#             spotify_service = SpotifyService()
#         service = spotify_service
#     else:
#         raise Exception('Unimplemented streaming service %s' % service_name)
#
#     return service


def main():
    print('Nothing doing')

    return 0


if __name__ == '__main__':
    exit_value = main()
    sys.exit(exit_value)
