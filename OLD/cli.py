
"""
Command-line interface functions, including parsing user input.
"""

import time
import re
import os.path

from data_model import *
import library_organizer
import rekordbox
import google_sheet
from streaming_service import StreamingService
from utils import *
import scripts


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
    if token in sheet.header:
        return "track.get('%s')" % token

    raise Exception("Unrecognizable field: '%s'" % token)


def eval_query(
        rekordbox_state: library_organizer.RekordboxState,
        sheet: google_sheet.GoogleSheet,
        tokens: list[str]
) -> Tracklist :
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

    result = Tracklist()
    for track in sheet:
        eval_result = eval(compiled_python_expr)
        if eval_result:
            result.append(track)

    return result


# PLAYLIST <name>
def handle_write_playlist_cmd(
        rekordbox_state: library_organizer.RekordboxState,
        playlist_name: str,
        playlist_dir: str,
        tracklist: Tracklist):
    if playlist_name.startswith("'") or playlist_name.startswith('"'):
        playlist_name = playlist_name[1:-1]
    playlist_filename = os.path.join(playlist_dir, playlist_name)

    if tracklist is None:
        raise Exception('No previous query')

    library_organizer.write_m3u_playlist(rekordbox_state, playlist_filename, tracklist)

# (SPOTIFY|YOUTUBE)
#     FIND/SEARCH ([QUERY]|<Rekordbox playlist name>)
#     CREATE PLAYLIST (FROM (QUERY|REKORDBOX? PLAYLIST <playlist name>))
def handle_streaming_service_cmd(
        batch_mode: bool,
        tokens: list[str],
        rekordbox_state: str,
        sheet: google_sheet.GoogleSheet,
        tracklist: Tracklist):
    assert len(tokens) >= 1 and tokens[0].upper() in ('SPOTIFY', 'YOUTUBE')

    service = library_organizer.get_streaming_service_by_name(tokens[0])

    tokens = tokens[1:]

    if len(tokens) == 0:
        raise Exception('No %s command' % service.name())

    if tokens[0].upper() in ['FIND', 'SEARCH']:
        handle_streaming_search(batch_mode, service, tokens[1:], rekordbox_state, sheet, tracklist)
        return

    if len(tokens) >= 2 and tokens[0].upper() == 'CREATE' and tokens[1].upper() == 'PLAYLIST':
        handle_streaming_create_playlist(batch_mode, service, tokens[2:], rekordbox_state, sheet, tracklist)
        return

    if tokens[0].upper() == 'PLAYLIST':
        handle_streaming_create_playlist(batch_mode, service, tokens[1:], rekordbox_state, sheet, tracklist)
        return

    raise Exception("Unknown %s command: '%s'" % (service.name(), ' '.join(tokens)))

# (SPOTIFY|YOUTUBE) FIND/SEARCH ([QUERY]|<Rekordbox playlist name>)
def handle_streaming_search(
        batch_mode: bool,
        service: StreamingService,
        tokens: list[str],
        rekordbox_state : library_organizer.RekordboxState,
        sheet: google_sheet.GoogleSheet,
        tracklist: Tracklist):
    if len(tokens) == 0:
        search_tracklist = sheet
    elif len(tokens) == 1:
        if tokens[0].upper() == 'QUERY':
            if tracklist is not None:
                search_tracklist = tracklist
            else:
                raise Exception('No previous query')
        else:
            playlist_name = tokens[0]
            if playlist_name.startswith("'") or playlist_name.startswith(('"')):
                playlist_name = playlist_name[1:-1]

            playlist = rekordbox_state.playlists.get(playlist_name)
            if playlist is None:
                raise Exception("Rekordbox playlist '%s' does not exist" % playlist_name)

            search_tracklist = [
                sheet.get_track_by_foreign_id('Rekordbox', playlist_track.id)
                for playlist_track in playlist
            ]
    else:
        raise Exception('Malformed %s SEARCH statement' % service.name())

    new_search_tracklist = []

    for track in search_tracklist:
        if track is not None and track.get('Spotify ID') is None:
            new_search_tracklist.append(track)

    new_search_tracklist.sort(key=lambda t: t.id)

    if len(new_search_tracklist) == 0:
        sys.stdout.write('No tracks to search for!\n')
        return

    print('Searching %s for %d tracks ...' % (service.name(), len(new_search_tracklist)))

    for track in new_search_tracklist:
        print('    Searching %s for rekordbox ID %d %s \u2013 %s' % (
            service.name(),
            track['Rekordbox ID'],
            ', '.join(track.artists),
            track.title
        ))

        possible_streaming_tracks = service.search(track)

        for idx, streaming_track in enumerate(possible_streaming_tracks):
            print('        Track #%d: %s ' % (
                (idx+1),
                streaming_track
            ))

            reply = get_user_choice(batch_mode, '        Is this it?', ['yes', 'no', 'next track', 'exit'])

            if reply == 'yes':
                track[service.name() + ' ID'] = streaming_track.id
                track.write_back()
                print('         Wrote back %s ID %s' % (service.name(), streaming_track.id))
                break
            elif reply == 'next track':
                print('        Giving up on this track!')
                break
            elif reply == 'exit':
                print('Giving up on all tracks!')
                return

        if track['Spotify ID'] is None:
            reply = get_user_choice(batch_mode, '        Unable to find %s URI for rekordbox ID %s %s \u2013 %s; mark as NOT FOUND?' % (
                service.name(),
                track['Rekordbox ID'],
                ', '.join(track.artists),
                track.title
            ))

            if reply == 'yes':
                track[service.name() + ' ID'] = 'NOT FOUND'
                track.write_back()
                print("        Wrote back URI 'NOT FOUND'")

    return

# (SPOTIFY|YOUTUBE) CREATE PLAYLIST (FROM (QUERY|REKORDBOX? PLAYLIST <playlist name>))
def handle_streaming_create_playlist(
        batch_mode: bool,
        service: StreamingService,
        tokens: list[str],
        rekordbox_state: library_organizer.RekordboxState,
        sheet: google_sheet.GoogleSheet,
        tracklist: Tracklist
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
            if len(tokens) == 2 and tokens[1].upper() == 'QUERY':
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

                    tracklist_to_write = rekordbox_playlist

    if tracklist_to_write is None:
        raise Exception('Malformed %s CREATE PLAYLIST command' % service.name())

    streaming_track_uris = []
    for track in tracklist_to_write:
        service_id_field = service.name() + ' ID'
        if service_id_field in track:
            service_id = track.get(service_id_field)
        else:
            sheet_track = sheet.get_track_by_foreign_id('Rekordbox', track.id)
            if sheet_track is not None:
                service_id = sheet_track.get(service_id_field)
            else:
                service_id = None

        if service_id is not None and service_id != '' and service_id != 'NOT FOUND':
            streaming_track_uris.append(service_id)

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
            batch_mode,
            "%s playlist '%s' exists - URI %s'" % (
                service.name(),
                streaming_playlist_name,
                streaming_playlist_uri
            ),
            ['enhance', 'overwrite', 'abort'])
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

        for service_track in existing_tracks:
            if service_track.id in streaming_track_uris:
                streaming_track_uris.remove(service_track.id)
            else:
                tracks_to_remove.append((service_track.id, str(service_track)))

        if len(tracks_to_remove) > 0:
            print("%s playlist '%s' - URI %s contains %d other tracks:" % (
                service.name(),
                streaming_playlist_name,
                streaming_playlist_uri,
                len(tracks_to_remove)
            ))
            for track_uri, track_description in tracks_to_remove:
                print('    %s - URI %s' % (track_description, track_uri))

            reply = get_user_choice(batch_mode, 'Remove?')

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

# SHOW
#     REKORDBOX? PLAYLISTS
#     (SPOTIFY|YOUTUBE) PLAYLISTS
#     TRACKS IN
#         (REKORDBOX? PLAYLIST)? <playlist name>
#         (SPOTIFY|YOUTUBE) PLAYLIST <playlist name>
def handle_show_cmd(
        tokens: list[str],
        rekordbox_state: library_organizer.RekordboxState) -> Tracklist:

    if len(tokens) == 0:
        raise Exception('No arguments to SHOW command')

    if ((len(tokens) == 1 and tokens[0].upper() == 'PLAYLISTS') or
            (len(tokens) == 2 and tokens[0].upper() == 'REKORDBOX' and tokens[1].upper() == 'PLAYLISTS')):
        print('Rekordbox playlists:')

        playlists = list(rekordbox_state.playlists.values())
        playlists.sort(key=lambda p: p.name)
        for playlist in playlists:
            print('    %s (%d tracks)' % (playlist.name, len(playlist)))

        return None

    if len(tokens) == 2 and tokens[0].upper() in ('SPOTIFY', 'YOUTUBE') and tokens[1].upper() == 'PLAYLISTS':
        service = library_organizer.get_streaming_service_by_name(tokens[0])

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

            print("Tracks in playlist '%s':" % playlist_name)

            for idx, track in enumerate(playlist):
                print('%d. %s \u2013 %s' % (
                    idx+1,
                    ', '.join(track.artists),
                    track.title
                ))

            return playlist
        else:
            # Streaming service playlist
            service = library_organizer.get_streaming_service_by_name(service_name)

            playlists = service.get_playlists()

            playlist_uri = playlists.get(playlist_name)

            if playlist_uri is None:
                raise Exception("%s playlist '%s' does not exist" % (service.name(), playlist_name))

            playlist = service.get_playlist_tracks(playlist_uri)

            for idx, track in enumerate(playlist):
                print('    %d: %s' % ((idx+1), track))

            return None

    raise Exception('Malformed SHOW command')

# Supported commands:
# - Q|X|QUIT|EXIT
# - FAIL ON EXCEPTION
# - WRITE M3U PLAYLIST <name>
# - (SPOTIFY|YOUTUBE)
#       (FIND|SEARCH) ([QUERY]|<Rekordbox playlist name>)
#       CREATE PLAYLIST (FROM (QUERY|REKORDBOX? PLAYLIST <playlist name>))
# - SHOW
#       REKORDBOX? PLAYLISTS
#       (SPOTIFY|YOUTUBE) PLAYLISTS
#       TRACKS IN
#           (REKORDBOX? PLAYLIST)? <playlist name>
#           (SPOTIFY|YOUTUBE) PLAYLIST <playlist name>
# - SCRIPT <script_name>
# - <query expression>
def cli_loop(
        batch_mode: bool,
        input_fh,
        rekordbox_state: library_organizer.RekordboxState,
        sheet: google_sheet.GoogleSheet,
        playlist_dir: str
):
    print('Ready to accept queries')
    tracklist: list[rekordbox.RekordboxTrack] = None
    fail_on_exception = (input_fh != sys.stdin)

    while (True):
        if input_fh == sys.stdin:
            sys.stdout.write('> ')
            sys.stdout.flush()
        query_text = input_fh.readline()

        if query_text == '':
            # EOF
            break

        query_text = query_text.strip()

        tokens = tokenize(query_text)

        if len(tokens) == 0:
            continue

        try:
            if len(tokens) == 1 and tokens[0].upper() in ['Q', 'X', 'QUIT', 'EXIT']:
                break

            if len(tokens) == 1 and tokens[0].upper() == 'BATCH':
                if batch_mode:
                    print('Already in batch mode')
                else:
                    print('Switching to batch mode')
                    batch_mode = True
                continue

            if len(tokens) == 1 and tokens[0].upper() == 'INTERACTIVE':
                if batch_mode:
                    print('Switching to interactive mode')
                    batch_mode = False
                else:
                    print('Already in interactive mode')
                continue

            if len(tokens) == 3 and (
                tokens[0].upper() == 'FAIL' and
                tokens[1].upper() == 'ON' and
                tokens[2].upper() == 'EXCEPTION'
                ):
                fail_on_exception = True
                continue

            if len(tokens) == 2 and tokens[0].upper() == 'RUN':
                filename = tokens[1]
                if filename[0] == "'":
                    filename = filename[1:-1]

                if filename[0] == '/':
                    full_filename = filename
                elif filename[:2] == '~/':
                    full_filename = os.environ['HOME'] + filename[1:]
                else:
                    full_filename = os.path.join(os.path.realpath(os.path.dirname(__file__)), 'scripts', filename)

                print('Executing file ' + full_filename)
                script_fh = open(full_filename, 'r')
                try:
                    cli_loop(
                        False,
                        script_fh,
                        rekordbox_state,
                        sheet,
                        playlist_dir
                    )
                finally:
                    script_fh.close()
                continue

            if len(tokens) == 4 and list(map(lambda x: x.upper(), tokens[:3])) == ['WRITE', 'M3U', 'PLAYLIST']:
                handle_write_playlist_cmd(rekordbox_state, tokens[3], playlist_dir, tracklist)
                continue

            if tokens[0].upper() == 'SHOW':
                tracklist = handle_show_cmd(tokens[1:], rekordbox_state)
                continue

            if tokens[0].upper() in ['SPOTIFY', 'YOUTUBE']:
                handle_streaming_service_cmd(batch_mode, tokens, rekordbox_state, sheet, tracklist)
                continue

            if len(tokens) >= 2 and tokens[0].upper() == 'SCRIPT':
                script_name = tokens[1]

                if not script_name in dir(scripts):
                    raise Exception('Unknown script %s' % script_name)

                getattr(scripts, script_name)(batch_mode, rekordbox_state, sheet, *tokens[2:])
                continue

            if len(tokens) >= 2 and tokens[0].upper() == 'QUERY':
                query_name = tokens[1]

                if not query_name in dir(scripts):
                    raise Exception('Unknown query %s' % query_name)

                tracklist = getattr(scripts, query_name)(rekordbox_state, sheet, *tokens[2:])
            else:
                tracklist = eval_query(rekordbox_state, sheet, tokens)

            for num, track in enumerate(tracklist):
                print('%d: %s' % (num+1, track))
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

