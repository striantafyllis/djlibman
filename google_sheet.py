
import sys
import os.path
import re

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
# from googleapiclient.errors import HttpError
from google.auth.exceptions import RefreshError

from data_model import *
from utils import *

SCOPES = ['https://www.googleapis.com/auth/drive']

CREDENTIALS_FILE = '/Users/spyros/google_credentials_music_library_management.json'

CACHED_TOKEN_FILE = 'google_cached_token.json'

DEFAULT_SPREADSHEET_ID = '1INf3kKkOQta6Im1mqMNFPxSQ1lRvFaMcQLrR1H2HP50'
# DEFAULT_SPREADSHEET_ID = '1G2_P_pj05owEmVZul9_wWLUjF38stWx5c4DFcipu3L0'

DEFAULT_SPREADSHEET_PAGE = 'Main Library'

TEST_SPREADSHEET_ID = '1pvJC8ThHEHHnz8-BRiS34pIDcq3j62Asj52cxP4Mwdk'

TEST_SPREADSHEET_PAGE = 'Sheet1'

google_service = None

required_attributes = [
    'Rekordbox ID',
    'Spotify ID',
    'YouTube ID',
    'Artists',
    'Title',
    'BPM',
    'Key',
    'Date Added'
]


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


class SheetTrack(Track):
    """In a Google track, the ID is the row in the Google sheet.
       Also, we keep track of the attributes whose value has changed so we can write them back."""
    sheet: object
    _dirty_attributes: set[str]

    def __init__(self, sheet, id, artists, title, attributes):
        super(SheetTrack, self).__init__(id, artists, title, attributes)
        self.sheet = sheet
        self._dirty_attributes = set()
        return

    def __setitem__(self, attribute, value):
        previous_value = self.get(attribute)

        super(SheetTrack, self).__setitem__(attribute, value)
        if attribute in self.sheet.header:
            self._dirty_attributes.add(attribute)

        # adjust the foreign key maps if necessary
        platform = self.sheet.foreign_id_attribute_to_platform.get(attribute)
        if platform is not None:
            assert previous_value is None  # No need to handle this currently
            self.sheet._update_foreign_id_map(self, platform, value)
        return

    def write_back(self):
        return self.sheet.write_back(self)

class GoogleSheet(Library):
    id: str
    page: str
    header: list[str]
    col_num_to_attribute: dict[int, str]
    attribute_to_col_num: dict[str, int]
    foreign_id_attribute_to_platform: dict[str, str]
    track_by_foreign_id: dict[str, dict[Union[int, str], SheetTrack]]

    def __init__(self, id, page, header):
        super(GoogleSheet, self).__init__('Google Main Library')
        self.id = id
        self.page = page
        self.header = header

        self.col_num_to_attribute = {}
        self.attribute_to_col_num = {}
        self.foreign_id_attribute_to_platform = {}

        self.track_by_foreign_id = defaultdict(dict[Union[int, str], SheetTrack])

        for col_num, attribute in enumerate(header):
            self.col_num_to_attribute[col_num] = attribute
            self.attribute_to_col_num[attribute] = col_num

            if re.match(r'[A-Za-z]+ ID', attribute):
                platform = attribute[:-3]
                self.foreign_id_attribute_to_platform[attribute] = platform

        missing_cols = []
        for required_attribute in required_attributes:
            if required_attribute not in self.col_num_to_attribute.values():
                missing_cols.append(attribute)

        if missing_cols != []:
            raise Exception('Sheet %s:%s has missing columns: %s' % (self.id, self.page, missing_cols))

    def append(self, track: SheetTrack):
        assert track.id == self.next_row()
        super(GoogleSheet, self).append(track)

        # add missing attributes in the header; this lets us avoid KeyErrors later
        for attribute in self.header:
            if attribute not in track:
                track[attribute] = None

        for foreign_id_attribute, platform in self.foreign_id_attribute_to_platform.items():
            foreign_id = track.get(foreign_id_attribute)
            self._update_foreign_id_map(track, platform, foreign_id)

        return

    def _update_foreign_id_map(self, track, platform, foreign_id):
        if foreign_id is None or foreign_id == 'NOT FOUND':
            return

        track_by_foreign_id = self.track_by_foreign_id[platform]
        if foreign_id in track_by_foreign_id:
            raise Exception('More than one Google Sheet track with the same %s ID %s: %s, %s' % (
                platform,
                foreign_id,
                track_by_foreign_id[foreign_id],
                track
            ))
        track_by_foreign_id[foreign_id] = track
        return

    def get_track_by_foreign_id(self, platform, foreign_id):
        return self.track_by_foreign_id[platform].get(foreign_id)

    def next_row(self):
        if len(self) == 0:
            return 2
        return self[-1].id+1

    def write_back(self, tracks=None):
        if tracks is None:
            tracks = self
        elif isinstance(tracks, SheetTrack):
            tracks = [tracks]

        data = []

        for track in tracks:
            for dirty_attribute in track._dirty_attributes:
                col_num = self.attribute_to_col_num.get(dirty_attribute)
                if col_num is None:
                    raise Exception("Unknown dirty attribute '%s'" % dirty_attribute)
                value = track[dirty_attribute]
                if isinstance(value, list):
                    value = ', '.join(value)

                if value is True:
                    value = 'T'
                elif value is False:
                    value = 'F'

                data.append({
                    'range': '%s!%s%d' % (self.page, col_num_to_alpha(col_num), track.id),
                    'values': [[value]]
                })

            track._dirty_attributes = set()

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

    sheet = GoogleSheet(spreadsheet_id, page, header)

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

    num_errors = 0

    for row_num in range(1, num_rows):
        row = values[row_num]

        attributes = {
            header[i]: infer_type(value)
            for i, value in enumerate(row)
        }

        artists_orig = attributes['Artists']
        title = attributes['Title']
        if artists_orig is None or title is None:
            sys.stderr.write('Row %d: Missing artists or title\n' % (row_num+1))
            num_errors += 1
            continue

        artists = frozenset(re.split(r' *[,&] *', artists_orig))

        track = SheetTrack(sheet, row_num + 1, artists, title, attributes)

        sheet.append(track)

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
