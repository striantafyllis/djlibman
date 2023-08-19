#!/usr/bin/env python

import sys
import argparse
import re
import time
import os.path

import rekordbox
import google_sheet
from streaming_service import StreamingService
from spotify_service import SpotifyService
from youtube_service import YouTubeService



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


def get_user_choice(prompt: str, options: list[str] = ['yes', 'no']):
    """Allows the user to choose among a number of options by typing any unambiguous prefix
    (usually the first letter) of an option"""
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

    time.sleep(0.1)

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

def sheet_stats(sheet: google_sheet.Sheet):
    print("Google sheet ID='%s' page='%s': %d entries" % (
        sheet.id,
        sheet.page,
        len(sheet.tracks)
    ))
    time.sleep(0.1)
    return


def cross_reference_rekordbox_to_google_sheet(
        rekordbox_state: RekordboxState,
        sheet: google_sheet.Sheet):

    num_errors = 0
    num_missing_ids = 0

    for track_info in sheet.tracks:
        if track_info.rekordbox_id is not None:
            track = rekordbox_state.collection.tracks_by_rekordbox_id.get(track_info.rekordbox_id)

            if track is None:
                sys.stderr.write('Sheet row %d: track ID %d not found in Rekordbox\n' % (
                    track_info.row_num, track_info.rekordbox_id))
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
                track_info.rekordbox_id = track.rekordbox_id
                track_info.dirty_fields.append('rekordbox_id')

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
        print('%d missing track IDs in Google sheet!' % num_missing_ids)
        reply = get_user_choice('Write missing IDs?')

        if reply == 'yes':
            updated_cells = sheet.write_back()
            print('*** Wrote %d missing IDs; verify manually! ***' % updated_cells)
            # sys.exit(1)
        else:
            sys.stderr.write('*** Fix missing IDs first! ***\n')
            sys.exit(1)

    return


def sheet_vs_rekordbox_sanity_checks(
        sheet: google_sheet.Sheet,
        rekordbox_state: RekordboxState):

    num_errors = 0
    num_tracks_missing_from_google_sheet = 0

    for track_info in sheet.tracks:
        if track_info.track not in rekordbox_state.main_library:
            sys.stderr.write('%s is in the Google sheet but not in Rekordbox Main Library\n' % track_info)
            num_errors += 1

    if num_errors > 0:
        sys.stderr.write('*** Fix the above %d errors manually! ***\n' % num_errors)
        sys.exit(1)

    for track in rekordbox_state.main_library.tracks:
        if track.track_info is None:
            print('%s is in Rekordbox Main Library but not in the Google sheet; adding.' % track)
            track_info = google_sheet.TrackInfo(sheet)
            for field in sheet.Track_field_to_col_num.keys():
                if field in dir(track):
                    setattr(track_info, field, getattr(track, field))
                    track_info.dirty_fields.append(field)
            track.track_info = track_info
            num_tracks_missing_from_google_sheet += 1

    if num_tracks_missing_from_google_sheet > 0:
        print('%d tracks missing from Google sheet!' % num_tracks_missing_from_google_sheet)
        reply = get_user_choice('Add missing tracks?')

        if reply == 'yes':
            updated_cells = sheet.write_back()
            print('*** Added %d missing tracks (%d cells); verify manually! ***' % (
                num_tracks_missing_from_google_sheet, updated_cells))
        else:
            sys.stderr.write('*** Fix missing tracks first! ***\n')
            sys.exit(1)

    num_mismatched_fields = 0

    for track_info in sheet.tracks:
        for field in sheet.Track_field_to_col_num.keys():
            if field not in dir(track_info.track):
                # Spotify/YouTube URIs are in TrackInfo but not in Track...
                continue
            sheet_value = getattr(track_info, field)
            rekordbox_value = getattr(track_info.track, field)
            if sheet_value != rekordbox_value:
                print("%s: field %s has value '%s' in Google sheet but '%s' in Rekordbox" % (
                    track_info, field, sheet_value, rekordbox_value
                ))
                num_mismatched_fields += 1

                setattr(track_info, field, rekordbox_value)
                track_info.dirty_fields.append(field)

    if num_mismatched_fields > 0:
        print('%d mismatching fields in Google sheet!' % num_mismatched_fields)
        reply = get_user_choice('Correct mismatched fields?')

        if reply == 'yes':
            updated_cells = sheet.write_back()
            print('*** Corrected %d mismatched fields; verify manually! ***' % updated_cells)
            # sys.exit(1)
        else:
            sys.stderr.write('*** Fix mismatched fields first! ***\n')
            sys.exit(1)


def tokenize(text):
    tokens = []

    while text != '':
        # eat whitespace
        m = re.match('\s*', text)
        text = text[m.end():]

        # match:
        # - operators - ==, <, >, <=, >=
        # - identifiers
        # - integers
        # - single-quoted strings
        # - double-quoted strings
        m = re.match(r"\(|\)|==|!=|<=|>=|<|>|&|\||[A-Za-z_][A-Za-z_0-9]*|[1-9][0-9]*|'[^']*'|\"[^\"]*\"", text)
        if m:
            tokens.append(m.group(0))
            text = text[m.end():]
        else:
            raise Exception("Unparseable suffix: '%s'" % text)

    return tokens

def convert_field_name(sheet, token):
    # see if it's a field name of the Track class
    track_field = google_sheet.col_name_to_Track_field.get(token)
    if track_field is not None:
        return 'track.%s' % track_field

    # otherwise, try to find it in the TrackInfo attributes
    if token in sheet.header:
        return "track.track_info.attributes.get('%s')" % token

    raise Exception("Unrecognizable field: '%s'" % token)

def eval_query(
        rekordbox_state: RekordboxState,
        sheet: google_sheet.Sheet,
        query_text: str
) -> list[rekordbox.Track] :
    tokens = tokenize(query_text)
    # print('Tokens: %s' % tokens)

    for i in range(len(tokens)):
        token = tokens[i]

        # convert boolean operators
        if token == '&':
            tokens[i] = 'and'
        elif token == '|':
            tokens[i] = 'or'
        elif re.match(r'[A-Za-z_][A-Za-z_0-9]*', token):
            # identifier
            if token == 'and' or token == 'or' or token == 'not':
                # boolean operator
                pass
            else:
                tokens[i] = convert_field_name(sheet, token)
        elif token[0] == '"':
            # double-quoted string; field name
            tokens[i] = convert_field_name(sheet, token[1:-1])

    python_expr = ' '.join(tokens)

    # print("Python expression: '%s'" % python_expr)

    compiled_python_expr = compile(python_expr, '<input>', 'eval')

    result = []
    for track in rekordbox_state.main_library.tracks:
        # TODO this should not be necessary if sanity checks have passed
        if track.track_info is None:
            continue
        eval_result = eval(compiled_python_expr)
        if eval_result:
            result.append(track)

    return result

def write_m3u_playlist(playlist_filename: str,
                       tracklist: list[rekordbox.Track]):
    if not playlist_filename.endswith('.m3u8'):
        playlist_filename += '.m3u8'

    playlist_file = open(playlist_filename, 'w')

    playlist_file.write('#EXTM3U\n')

    for track in tracklist:
        playlist_file.write('#EXTINF:%d,%s \u2013 %s\n' % (
            track.duration,
            track.artist_orig,
            track.title
        ))
        playlist_file.write(track.location + '\n')

    playlist_file.close()

    print("Wrote file '%s' (%d tracks)" % (playlist_filename, len(tracklist)))


def handle_playlist_cmd(playlist_cmd: str,
                        playlist_dir: str,
                        tracklist: list[rekordbox.Track]):
    m = re.match('PLAYLIST\s+([A-Z].*)', playlist_cmd, re.IGNORECASE)
    if not m:
        raise Exception('Malformed PLAYLIST command')
    playlist_name = m.group(1)
    playlist_filename = os.path.join(playlist_dir, playlist_name)

    if tracklist is None:
        raise Exception('No previous query')

    write_m3u_playlist(playlist_filename, tracklist)
    tracklist = None

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


def handle_streaming_service_cmd(streaming_cmd: str,
                                 rekordbox_state: str,
                                 tracklist: list[rekordbox.Track]):
    tokens = tokenize(streaming_cmd)

    assert len(tokens) >= 1 and tokens[0].upper() in ('SPOTIFY', 'YOUTUBE')

    service = get_streaming_service_by_name(tokens[0])

    tokens = tokens[1:]

    if len(tokens) == 0:
        raise Exception('No %s command' % service.name())

    if tokens[0].upper() in ['FIND', 'SEARCH']:
        handle_streaming_search(service, tokens[1:], rekordbox_state, tracklist)
        return

    if len(tokens) >= 2 and tokens[0].upper() == 'CREATE' and tokens[1].upper() == 'PLAYLIST':
        handle_streaming_create_playlist(service, tokens[2:], rekordbox_state, tracklist)
        return

    if tokens[0].upper() == 'PLAYLIST':
        handle_streaming_create_playlist(service, tokens[1:], rekordbox_state, tracklist)
        return

    raise Exception("Unknown %s command: '%s'" % (service.name(), ' '.join(tokens)))

def handle_streaming_search(service: StreamingService,
                            tokens: list[str],
                            rekordbox_state : RekordboxState,
                            tracklist: list[rekordbox.Track]):
    if len(tokens) == 1 and tokens[0].upper() == 'QUERY':
        if tracklist is not None:
            search_tracklist = tracklist
        else:
            raise Exception('No previous query')
    else:
        if len(tokens) == 0:
            playlist_name = 'Main Library'
        else:
            playlist_name = ' '.join(tokens)

        search_tracklist = rekordbox_state.playlists.get(playlist_name)
        if search_tracklist is None:
            raise Exception("Rekordbox playlist '%s' does not exist" % playlist_name)

    search_trackinfo_list = [
        track.track_info
        for track in search_tracklist.tracks
        if track.track_info is not None
           and track.track_info.spotify_uri is None
    ]

    search_trackinfo_list.sort(key=lambda t: t.row_num)

    if len(search_trackinfo_list) == 0:
        sys.stderr.write('No tracks to search for!\n')

    print('Searching %s for %d tracks ...' % (service.name(), len(search_trackinfo_list)))

    for track_info in search_trackinfo_list:
        print('    Searching %s for rekordbox ID %d %s \u2013 %s' % (
            service.name(),
            track_info.rekordbox_id,
            ', '.join(track_info.artists),
            track_info.title
        ))

        possible_streaming_tracks = service.search(track_info)

        for idx, (track_uri, track_description) in enumerate(possible_streaming_tracks):
            print('        Track #%d: %s ' % (
                (idx+1),
                track_description
            ))

            reply = get_user_choice('        Is this it?', ['yes', 'no', 'next track', 'exit'])

            if reply == 'yes':
                track_info.spotify_uri = track_uri
                track_info.dirty_fields.append('spotify_uri')
                track_info.write_back()
                print('         Wrote back Spotify URI %s' % track_uri)
                break
            elif reply == 'next track':
                print('        Giving up on this track!')
                break
            elif reply == 'exit':
                print('Giving up on all tracks!')
                return

        if track_info.spotify_uri is None:
            reply = get_user_choice('        Unable to find Spotify URI for rekordbox ID %s %s \u2013 %s; mark as NOT FOUND?' % (
                track_info.rekordbox_id,
                ', '.join(track_info.artists),
                track_info.title
            ))

            if reply.upper() == 'yes':
                track_info.spotify_uri = 'NOT FOUND'
                track_info.dirty_fields.append('spotify_uri')
                track_info.write_back()
                print("        Wrote back URI 'NOT FOUND'")

    return

def handle_streaming_create_playlist(
        service: StreamingService,
        tokens: list[str],
        rekordbox_state: RekordboxState,
        tracklist: list[rekordbox.Track]
):
    if len(tokens) < 1:
        raise Exception('Malformed %s CREATE PLAYLIST command' % service.name())

    streaming_playlist_name = tokens[0]

    if streaming_playlist_name.startswith('"') or streaming_playlist_name.startswith("'"):
        streaming_playlist_name = streaming_playlist_name[1:-1]

    tokens = tokens[1:]

    tracklist_to_write = None

    if len(tokens) == 0:
        if tracklist is None:
            raise Exception('No previous query')
        tracklist_to_write = tracklist

    if len(tokens) >= 1 and tokens[0].upper() == 'FROM':
        if len(tokens) >= 2:
            if len(tokens) == 2 and tokens[1] == 'QUERY':
                if tracklist is None:
                    raise Exception('No previous query')
                tracklist_to_write = tracklist
            else:
                playlist_name = None

                if len(tokens) == 2:
                    playlist_name = tokens[1]
                elif len(tokens) == 3 and tokens[1].upper() == 'PLAYLIST':
                    playlist_name = tokens[2]
                elif len(tokens) == 4 and tokens[1].upper() == 'REKORDBOX' and tokens[2].upper() == 'PLAYLIST':
                    playlist_name = tokens[3]

                if playlist_name is not None:
                    if playlist_name.startswith('"') or playlist_name.startswith("'"):
                        playlist_name = playlist_name[1:-1]

                    rekordbox_playlist = rekordbox_state.playlists.get(playlist_name)

                    if rekordbox_playlist is None:
                        raise Exception("Rekordbox playlist '%s' not found" % playlist_name)

                    tracklist_to_write = rekordbox_playlist.tracks

                    # especially for the Main Library, use the ordering of the Google doc
                    if playlist_name == 'Main Library':
                        tracklist_to_write = list(tracklist_to_write)
                        tracklist_to_write.sort(key=lambda t: t.track_info.row_num if t.track_info is not None else -1)

    if tracklist_to_write is None:
        raise Exception('Malformed %s CREATE PLAYLIST command' % service.name())

    streaming_track_uris = [
        service.get_TrackInfo_field(track.track_info)
        for track in tracklist_to_write
        if track.track_info is not None
    ]

    streaming_track_uris = [uri
                            for uri in streaming_track_uris
                            if uri is not None and uri != 'NOT FOUND']

    print("Writing %d tracks to %s playlist '%s' (%d tracks omitted because of missing %s URIs)" % (
      len(streaming_track_uris),
      service.name(),
      streaming_playlist_name,
      len(tracklist_to_write) - len(streaming_track_uris),
        service.name()
    ))

    if len(streaming_track_uris) == 0:
        raise Exception('No tracks to write')

    # check that the streaming service playlist doesn't exist
    streaming_playlists = service.get_playlists()

    streaming_playlist_uri = streaming_playlists.get(streaming_playlist_name)

    if streaming_playlist_uri is not None:
        reply = get_user_choice(
            "%s playlist '%s' exists - URI %s'" % (
                service.name(),
                streaming_playlist_name,
                streaming_playlist_uri
            ),
            ['overwrite', 'enhance', 'abort'])
    else:
        reply = 'does not exist'

    if reply == 'abort':
        print("Aborting - %s playlist '%s' - URI %s left as-is" % (
            service.name(), streaming_playlist_name, streaming_playlist_uri))
        return

    if reply == 'overwrite':
        print("Deleting %s playlist '%s' - URI %s" % (
            service.name(),
            streaming_playlist_name,
            streaming_playlist_uri))
        service.delete_playlist(streaming_playlist_uri)

    if reply == 'overwrite' or reply == 'does not exist':
        streaming_playlist_uri = service.create_playlist(streaming_playlist_name)
        print("Created %s playlist '%s' - URI %s" % (
            service.name(),
            streaming_playlist_name,
            streaming_playlist_uri))

    if reply == 'enhance':
        existing_tracks = service.get_playlist_tracks(streaming_playlist_uri)

        tracks_to_remove = []

        for track_uri, track_description in existing_tracks:
            if track_uri in streaming_track_uris:
                streaming_track_uris.remove(track_uri)
            else:
                tracks_to_remove.append((track_uri, track_description))

        if len(tracks_to_remove) > 0:
            print("%s playlist '%s' - URI %s contains %d other tracks:" % (
                service.name(),
                streaming_playlist_name,
                streaming_playlist_uri,
                len(tracks_to_remove)
            ))
            for track_uri, track_description in tracks_to_remove:
                print('    %s - URI %s' % (track_description, track_uri))

            reply = get_user_choice('Remove?')

            if reply == 'yes':
                service.remove_tracks_from_playlist(
                    streaming_playlist_uri,
                    [track_uri for track_uri, _ in tracks_to_remove]
                )
            print("Removed %d tracks from %s playlist '%s' - URI %s" % (
                len(tracks_to_remove),
                service.name(),
                streaming_playlist_name,
                streaming_playlist_uri
            ))

    service.add_tracks_to_playlist(streaming_playlist_uri, streaming_track_uris)

    print("Added %d tracks to %s playlist '%s'" % (
        len(streaming_track_uris),
        service.name(),
        streaming_playlist_name))

def handle_show_cmd(
        query_text: str,
        rekordbox_state: RekordboxState) -> list[rekordbox.Track]:
    tokens = tokenize(query_text)

    assert len(tokens) >= 1 and tokens[0].upper() == 'SHOW'

    tokens = tokens[1:]

    if len(tokens) == 0:
        raise Exception('No arguments to SHOW command')

    if ((len(tokens) == 1 and tokens[0].upper() == 'PLAYLISTS') or
            (len(tokens) == 2 and tokens[0].upper() == 'REKORDBOX' and tokens[1].upper() == 'PLAYLISTS')):
        print('Rekordbox playlists:')

        playlists = list(rekordbox_state.playlists.values())
        playlists.sort(key=lambda p: p.name)
        for playlist in playlists:
            print('    %s (%d tracks)' % (playlist.name, len(playlist.tracks)))

        return None

    if len(tokens) == 2 and tokens[0].upper() in ('SPOTIFY', 'YOUTUBE') and tokens[1].upper() == 'PLAYLISTS':
        service = get_streaming_service_by_name(tokens[0])

        playlists = list(service.get_playlists().items())

        playlists.sort(key = lambda p: p[0])

        print('%s playlists:' % service.name())
        for name, uri in playlists:
            print('    %s -- URI %s' % (name, uri))

        return None

    if len(tokens) >= 3 and tokens[0].upper() == 'TRACKS' and tokens[1].upper() == 'IN':
        if len(tokens) == 5 and tokens[2].upper() in ['REKORDBOX', 'SPOTIFY', 'YOUTUBE'] and tokens[3].upper() == 'PLAYLIST':
            service_name = tokens[2]
            playlist_name = tokens[4]
        elif len(tokens) == 4 and tokens[2].upper() == 'PLAYLIST':
            service_name = 'Rekordbox'
            playlist_name = tokens[3]
        elif len(tokens) == 3:
            service_name = 'Rekordbox'
            playlist_name = tokens[2]
        else:
            raise Exception('Malformed SHOW TRACKS command')

        if playlist_name.startswith("'") or playlist_name.startswith('"'):
            playlist_name = playlist_name[1:-1]

        if service_name.upper() == 'REKORDBOX':
            # Rekordbox playlist
            playlist = rekordbox_state.playlists.get(playlist_name)

            if playlist is None:
                raise Exception("Playlist '%s' not found" % playlist_name)

            tracklist = playlist.tracks

            print("Tracks in playlist '%s':" % playlist_name)

            for idx, track in enumerate(tracklist):
                print('    %d: %s \u2013 %s' % (
                    idx+1,
                    ', '.join(track.artists),
                    track.title
                ))

            return tracklist
        else:
            # Streaming service playlist
            service = get_streaming_service_by_name(service_name)

            playlists = service.get_playlists()

            playlist_uri = playlists.get(playlist_name)

            if playlist_uri is None:
                raise Exception("%s playlist '%s' does not exist" % (service.name(), playlist_name))

            playlist_tracks = service.get_playlist_tracks(playlist_uri)

            for idx, (_, description) in enumerate(playlist_tracks):
                print('    %d: %s' % ((idx+1), description))

            return None

    raise Exception('Malformed SHOW command')


def main():
    parser = argparse.ArgumentParser(
        prog='library_organizer.py',
        description='Organizes my music library between Rekordbox, Spotify and YouTube with help from some Google docs'
    )
    parser.add_argument('--playlist_dir', default=rekordbox.default_playlist_dir)
    parser.add_argument('--rekordbox_xml', default=rekordbox.default_rekordbox_xml)
    parser.add_argument('--google_sheet_id', default=google_sheet.DEFAULT_SPREADSHEET_ID)
    parser.add_argument('--google_sheet_page', default=google_sheet.DEFAULT_SPREADSHEET_PAGE)

    args = parser.parse_args()

    playlist_dir = args.playlist_dir

    rekordbox_state = read_rekordbox(args.rekordbox_xml)

    sheet = google_sheet.parse_sheet(args.google_sheet_id, args.google_sheet_page)

    rekordbox_stats(rekordbox_state)
    rekordbox_sanity_checks(rekordbox_state)

    sheet_stats(sheet)

    cross_reference_rekordbox_to_google_sheet(rekordbox_state, sheet)

    sheet_vs_rekordbox_sanity_checks(sheet, rekordbox_state)

    print('Ready to accept queries')
    tracklist: list[rekordbox.Track] = None
    fail_on_exception = False
    while (True):
        sys.stdout.write('> ')
        sys.stdout.flush()
        query_text = sys.stdin.readline()

        if query_text == '':
            # EOF
            break

        query_text = query_text.strip()

        if query_text == '':
            continue

        try:
            if re.match(r'q|x|quit|exit$', query_text, re.IGNORECASE):
                break

            if re.match(r'fail\s+on\s+exception', query_text, re.IGNORECASE):
                fail_on_exception = True
                continue

            if re.match(r'playlist\s+', query_text, re.IGNORECASE):
                handle_playlist_cmd(query_text, playlist_dir, tracklist)
                continue

            if re.match(r'show\s+', query_text, re.IGNORECASE):
                tracklist = handle_show_cmd(query_text, rekordbox_state)
                continue

            if re.match(r'(spotify|youtube)\s+', query_text, re.IGNORECASE):
                handle_streaming_service_cmd(query_text, rekordbox_state, tracklist)
                continue

            tracklist = eval_query(rekordbox_state, sheet, query_text)
            for track in tracklist:
                print('%s \u2013 %s' % (track.artist_orig, track.title))
            print('(%d tracks)' % len(tracklist))
        except Exception as e:
            sys.stdout.flush()
            sys.stderr.write('Query "%s" failed: %s\n' % (query_text, e))
            sys.stderr.flush()
            time.sleep(0.1)
            print()
            if fail_on_exception:
                raise e
            continue

    return 0




if __name__ == '__main__':
    exit_value = main()
    sys.exit(exit_value)
