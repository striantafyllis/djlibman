
import sys
import os.path
import re

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import rekordbox

SCOPES = ['https://www.googleapis.com/auth/drive']

CREDENTIALS_FILE = '/Users/spyros/google_credentials_music_library_management.json'

DEFAULT_SPREADSHEET_ID = '11vIt1o-WB63XxdtSCfH8eOJ0vScCHJ-xzL0MphNqAIc'

DEFAULT_SPREADSHEET_PAGE = 'Main Library'

google_service = None

col_name_to_Track_field = {
    'ID': 'id',
    'Artists': 'artists',
    'Title': 'title',
    'BPM': 'bpm',
    'Key': 'key',
    'Date Added': 'date_added'
}


def col_num_to_name(col_num):
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
    row_num: int
    id: int
    artists: list[str]
    title: str
    bpm: float
    key: str
    date_added: str
    attributes: dict
    track: rekordbox.Track = None
    dirty_fields: list[str]

    def __init__(self,
                 row_num: int):
        self.row_num = row_num
        self.id = None
        self.artists = None
        self.title = None
        self.bpm = None
        self.key = None
        self.date_added = None
        self.attributes = {}
        self.track = None
        self.dirty_fields = []
        return

    def __str__(self):
        return 'TrackInfo row=%d id=%s artists=%s title=%s' % (self.row_num, self.id, self.artists, self.title)


class Sheet:
    id: str
    page: str
    header: list[str]
    tracks: list[TrackInfo]
    col_num_to_Track_field: dict[int, str]
    Track_field_to_col_num: dict[str, int]

    def __init__(self, id, page, header):
        self.id = id
        self.page = page
        self.header = header

        self.tracks = []

        self.col_num_to_Track_field = {}
        self.Track_field_to_col_num = {}

        for col_num in range(len(header)):
            col_name = header[col_num]
            Track_field = col_name_to_Track_field.get(col_name)
            if Track_field is not None:
                self.col_num_to_Track_field[col_num] = Track_field
                self.Track_field_to_col_num[Track_field] = col_num

        missing_cols = []
        for col_name, Track_field in col_name_to_Track_field.items():
            if Track_field not in self.col_num_to_Track_field.values():
                missing_cols.append(col_name)

        if missing_cols != []:
            raise Exception('Sheet %s:%s has missing columns: %s' % (self.id, self.page, missing_cols))

    def write_back(self):
        data = []

        for track in self.tracks:
            for dirty_field in track.dirty_fields:
                col_num = self.Track_field_to_col_num[dirty_field]
                data.append({
                    'range': '%s!%s%d' % (self.page, col_num_to_name(col_num), track.row_num),
                    'values': [[getattr(track, dirty_field)]]
                })

            track.dirty_fields = []

        if data == []:
            return 0

        result = google_service.spreadsheets().values().batchUpdate(
            spreadsheetId=DEFAULT_SPREADSHEET_ID,
            body={
                'valueInputOption': 'RAW',
                'data': data
            }
        ).execute()

        return result['totalUpdatedCells']


def init_service():
    global google_service

    if google_service is not None:
        return

    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                '/Users/spyros/google_credentials_music_library_management.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
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
            elif row[j] == 'x':
                row[j] = True
            elif int_regex.match(row[j]):
                row[j] = int(row[j])
            elif float_regex.match(row[j]):
                row[j] = float(row[j])

    num_errors = 0

    for row_num in range(1, num_rows):
        track_info = TrackInfo(row_num+1)
        sheet.tracks.append(track_info)

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
            'range': DEFAULT_SPREADSHEET_PAGE + '!A2',
            'values': [[6]]
        },
        {
            'range': DEFAULT_SPREADSHEET_PAGE + '!A3',
            'values': [[12]]
        },
        {
            'range': DEFAULT_SPREADSHEET_PAGE + '!A4',
            'values': [[18]]
        }
    ]

    result = google_service.spreadsheets().values().batchUpdate(
        spreadsheetId = DEFAULT_SPREADSHEET_ID,
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
