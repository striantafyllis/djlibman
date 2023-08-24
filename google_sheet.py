
import sys
import os.path
import re

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
# from googleapiclient.errors import HttpError
from google.auth.exceptions import RefreshError

import rekordbox

SCOPES = ['https://www.googleapis.com/auth/drive']

CREDENTIALS_FILE = '/Users/spyros/google_credentials_music_library_management.json'

CACHED_TOKEN_FILE = 'google_cached_token.json'

DEFAULT_SPREADSHEET_ID = '1INf3kKkOQta6Im1mqMNFPxSQ1lRvFaMcQLrR1H2HP50'
# DEFAULT_SPREADSHEET_ID = '1G2_P_pj05owEmVZul9_wWLUjF38stWx5c4DFcipu3L0'

DEFAULT_SPREADSHEET_PAGE = 'Main Library'

TEST_SPREADSHEET_ID = '1pvJC8ThHEHHnz8-BRiS34pIDcq3j62Asj52cxP4Mwdk'

TEST_SPREADSHEET_PAGE = 'Sheet1'

google_service = None

col_name_to_Track_field = {
    'Rekordbox ID': 'rekordbox_id',
    'Spotify URI': 'spotify_uri',
    'YouTube URI': 'youtube_uri',
    'Artists': 'artists',
    'Title': 'title',
    'BPM': 'bpm',
    'Key': 'key',
    'Date Added': 'date_added'
}


def col_num_to_alpha(col_num):
    if col_num < 26:
        return chr(ord('A') + col_num)
    else:
        first_letter_num = col_num // 26 - 1
        if first_letter_num < 26:
            first_letter = chr(ord('A') + first_letter_num)
        else:
            raise Exception('Column number too big: %d' % col_num)
        second_letter_num = col_num % 26
        second_letter = chr(ord('A') + second_letter_num)

        return first_letter + second_letter


class TrackInfo:
    sheet: object
    row_num: int
    rekordbox_id: int
    spotify_uri: str
    youtube_uri: str
    artists: list[str]
    title: str
    bpm: float
    key: str
    date_added: str
    attributes: dict
    track: rekordbox.Track = None
    dirty_fields: set[str]

    def __init__(self, sheet, row_num=None):
        self.sheet = sheet
        self.row_num = row_num
        self.rekordbox_id = None
        self.spotify_uri = None
        self.artists = None
        self.title = None
        self.bpm = None
        self.key = None
        self.date_added = None
        self.attributes = {}
        self.track = None
        self.dirty_fields = set()

        sheet.add_track(self)
        return

    def __str__(self):
        return 'TrackInfo row=%d id=%s artists=%s title=%s' % (self.row_num, self.rekordbox_id, self.artists, self.title)

    def write_back(self):
        self.sheet.write_back([self])
        return


class Sheet:
    id: str
    page: str
    header: list[str]
    tracks: list[TrackInfo]
    next_row: int
    col_num_to_Track_field: dict[int, str]
    Track_field_to_col_num: dict[str, int]
    col_num_to_attribute: dict[int, str]
    attribute_to_col_num: dict[str, int]

    def __init__(self, id, page, header):
        self.id = id
        self.page = page
        self.header = header

        self.tracks = []

        self.next_row = 2

        self.col_num_to_Track_field = {}
        self.Track_field_to_col_num = {}
        self.col_num_to_attribute = {}
        self.attribute_to_col_num = {}

        for col_num in range(len(header)):
            col_name = header[col_num]
            Track_field = col_name_to_Track_field.get(col_name)
            if Track_field is not None:
                self.col_num_to_Track_field[col_num] = Track_field
                self.Track_field_to_col_num[Track_field] = col_num
            else:
                self.col_num_to_attribute[col_num] = col_name
                self.attribute_to_col_num[col_name] = col_num

        missing_cols = []
        for col_name, Track_field in col_name_to_Track_field.items():
            if Track_field not in self.col_num_to_Track_field.values():
                missing_cols.append(col_name)

        if missing_cols != []:
            raise Exception('Sheet %s:%s has missing columns: %s' % (self.id, self.page, missing_cols))

    def add_track(self, track: TrackInfo):
        self.tracks.append(track)
        if track.row_num is not None:
            self.next_row = max(self.next_row, track.row_num+1)
        else:
            track.row_num = self.next_row
            self.next_row += 1

    def write_back(self, tracks=None):
        if tracks is None:
            tracks = self.tracks

        data = []

        for track in tracks:
            for dirty_field in track.dirty_fields:
                col_num = self.Track_field_to_col_num.get(dirty_field)
                if col_num is not None:
                    value = getattr(track, dirty_field)
                else:
                    col_num = self.attribute_to_col_num.get(dirty_field)
                    if col_num is None:
                        raise Exception("Unknown dirty field '%s'" % dirty_field)
                    value = track.attributes[dirty_field]
                if isinstance(value, list):
                    value = ', '.join(value)

                if value is True:
                    value = 'T'
                elif value is False:
                    value = 'F'

                data.append({
                    'range': '%s!%s%d' % (self.page, col_num_to_alpha(col_num), track.row_num),
                    'values': [[value]]
                })

            track.dirty_fields = []

        if data == []:
            return 0

        body = {
                'valueInputOption': 'USER_ENTERED',
                'data': data
            }

        result = google_service.spreadsheets().values().batchUpdate(
            spreadsheetId=DEFAULT_SPREADSHEET_ID,
            body=body
        ).execute()

        return result['totalUpdatedCells']


def init_service():
    global google_service

    if google_service is not None:
        return

    if os.path.exists(CACHED_TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(CACHED_TOKEN_FILE, SCOPES)
    else:
        creds = None
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except RefreshError:
                os.unlink(CACHED_TOKEN_FILE)
                creds = None

        if not creds or not creds.valid:
            flow = InstalledAppFlow.from_client_secrets_file(
                '/Users/spyros/google_credentials_music_library_management.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(CACHED_TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())

    google_service = build('sheets', 'v4', credentials=creds)

    return

def parse_sheet(spreadsheet_id = DEFAULT_SPREADSHEET_ID, page = DEFAULT_SPREADSHEET_PAGE):
    global google_service

    init_service()

    google_sheet = google_service.spreadsheets()
    result = google_sheet.values().get(spreadsheetId=spreadsheet_id, range = page).execute()
    values = result.get('values', [])

    num_rows = len(values)

    if num_rows == 0:
        raise Exception('Empty spreadsheet (no header row)')

    header = values[0]

    sheet = Sheet(spreadsheet_id, page, header)

    # these are simpler than usual because more complicated values don't appear
    int_regex = re.compile(r'^[1-9][0-9]*^')
    float_regex = re.compile(r'^[1-9][0-9]*(\.[0-9]+)?$')

    # do some basic post-processing
    for i in range(num_rows):
        row = values[i]
        for j in range(len(row)):
            if row[j] is None:
                continue
            if row[j] == '':
                row[j] = None
            elif row[j] == 'T':
                row[j] = True
            elif row[j] == 'F':
                row[j] = False
            elif int_regex.match(row[j]):
                row[j] = int(row[j])
            elif float_regex.match(row[j]):
                row[j] = float(row[j])

    num_errors = 0

    for row_num in range(1, num_rows):
        track_info = TrackInfo(sheet, row_num+1)

        row = values[row_num]

        for col_num in range(len(row)):
            value = row[col_num]

            Track_field = sheet.col_num_to_Track_field.get(col_num)
            if Track_field is not None:
                if Track_field == 'artists':
                    value = re.split(r' *[,&] *', value)
                setattr(track_info, Track_field, value)
            else:
                track_info.attributes[header[col_num]] = value

        # make sure some fields are always there - otherwise we can't find the song
        if track_info.artists is None or track_info.title is None:
            sys.stderr.write('Row %d: Missing artists or title\n' % (row_num+1))
            num_errors += 1

    if num_errors > 0:
        sys.stderr.write('*** Fix the above %d errors manually! ***\n' % num_errors)
        sys.exit(1)

    return sheet

def test_write():
    init_service()

    global google_service

    init_service()

    data = [
        {
            'range': TEST_SPREADSHEET_PAGE + '!A2',
            'values': [[6]]
        },
        {
            'range': TEST_SPREADSHEET_PAGE + '!A3',
            'values': [[12]]
        },
        {
            'range': TEST_SPREADSHEET_PAGE + '!A4',
            'values': [['2023-08-23']]
        }
    ]

    result = google_service.spreadsheets().values().batchUpdate(
        spreadsheetId = TEST_SPREADSHEET_ID,
        body = {
            'valueInputOption': 'USER_ENTERED',
            'data': data
        }
    ).execute()

    print('Result: %s' % result)

    return


def main():
    # sheet = parse_sheet()
    test_write()
    return 0

if __name__ == '__main__':
    sys.exit(main())
