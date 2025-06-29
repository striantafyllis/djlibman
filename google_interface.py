import os.path
import re
import sys
import time

import pandas as pd

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
# from googleapiclient.errors import HttpError
from google.auth.exceptions import RefreshError

from file_interface import FileDoc

from general_utils import *

_SCOPES = 'https://www.googleapis.com/auth/drive'


def _col_num_to_alpha(col_num):
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


class GoogleInterface:
    def __init__(self, config):
        self._credentials = config['credentials']
        self._cached_token_file = config['cached_token_file']

        for field in config.keys():
            if field not in ['credentials', 'cached_token_file']:
                raise Exception('Unknown field in config section google: %s' % field)

        # the connection will be initialized when it's first used
        self._drive_connection = None
        self._sheets_connection = None
        return

    def _init_connection(self):
        if self._drive_connection is not None:
            return

        if os.path.exists(self._cached_token_file):
            creds = Credentials.from_authorized_user_file(self._cached_token_file, _SCOPES)
        else:
            creds = None
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                except RefreshError:
                    os.unlink(self._cached_token_file)
                    creds = None

            if not creds or not creds.valid:
                flow = InstalledAppFlow.from_client_secrets_file(
                    '/Users/spyros/google_credentials_music_library_management.json', _SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(self._cached_token_file, 'w') as token:
                token.write(creds.to_json())

        self._drive_connection = build('drive', 'v3', credentials=creds)
        self._sheets_connection = build('sheets', 'v4', credentials=creds)
        return

    def drive_connection(self):
        self._init_connection()
        return self._drive_connection

    def sheets_connection(self):
        self._init_connection()
        return self._sheets_connection

    def get_files(self, type=None, name=None):
        conn = self.drive_connection()

        query = None
        if type is not None:
            query = 'mimeType='
            if type == 'sheet':
                query += "'application/vnd.google-apps.spreadsheet'"
            elif type == 'doc':
                query += "'application/vnd.google-apps.document'"
            else:
                raise ValueError(f"Unknown Google file type: '{type}'")

        if name is not None:
            if query is None:
                query = ''
            else:
                query += ' and '

            query += f"name='{name}'"

        results = conn.files().list(q=query).execute()

        files = results.get('files', [])

        nextPageToken = results.get('nextPageToken')

        while nextPageToken is not None:
            results = conn.files().list(pageToken=nextPageToken).execute()

            files += results.get('files', [])
            nextPageToken = results.get('nextPageToken')

        return files

_converters = [
    np.int64,
    np.float64,
    lambda s: pd.to_datetime(s, utc=True)
]


def _convert_read_value(value):
    if value is None or value == '':
        return None

    for conv in _converters:
        try:
            new_value = conv(value)
            return new_value
        except ValueError:
            continue

    return value


class GoogleSheet(FileDoc):
    def __init__(self,
                 google_interface,
                 path,
                 sheet,
                 **kwargs
                 ):
        super(GoogleSheet, self).__init__(path, **kwargs)
        self._interface = google_interface
        self._sheet = sheet
        self._id = None
        return

    def _init_id(self):
        if self._id is not None:
            return

        files = self._interface.get_files(type='sheet', name=self._path)

        if len(files) == 0:
            self._id = '_DOESNT_EXIST'
        elif len(files) == 1:
            self._id = files[0]['id']
        else:
            raise ValueError(f"More than one Google Sheets match name '{self._path}'")

    def exists(self):
        self._init_id()
        return self._id != '_DOESNT_EXIST'

    def getmtime(self):
        self._init_id()
        if self._id == '_DOESNT_EXIST':
            return None

        result = self._interface.drive_connection().files()\
            .get(fileId=self._id, fields='modifiedTime')\
            .execute()

        return pd.Timestamp(result['modifiedTime']).timestamp()

    def _back_up(self):
        # no need to back up as Google keeps version history
        return

    def delete_backups(self):
        return

    def _raw_read(self):
        result = self._interface.sheets_connection().spreadsheets()\
            .values().get(spreadsheetId=self._id, range=self._sheet)\
            .execute()

        values = result.get('values')

        num_columns = max([len(row) for row in values])

        if self._header is not None:
            if len(values) < 1:
                raise ValueError(f'Google sheet {self._path} page {self._sheet}: '
                                 f'header specified but sheet has no rows')
            columns = values[0]
            values = values[1:]

            if len(columns) < num_columns:
                raise ValueError(
                    f'Google sheet {self._path} page {self._sheet}: '
                    f'header has {len(columns)} columns but data has {num_columns} columns')
        else:
            columns = [_col_num_to_alpha(col_num) for col_num in range(num_columns)]

        # Google returns everything as strings. This code tries to fix the mess.

        conv_values = [
            [_convert_read_value(v) for v in row]
            for row in values
        ]

        df = pd.DataFrame(data=conv_values, columns=columns)

        # this is necessary because int columns will show up as floats
        df2 = df.apply(lambda column: column.convert_dtypes(), axis=0)

        return df2

    def _raw_write(self, df):
        raise Exception('Not implemented')

    def delete(self):
        raise Exception('Deleting Google sheets not supported')

    def write(self, df):
        raise Exception('Google sheet writing not supported yet')

