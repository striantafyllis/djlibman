import os.path
import re
import sys

import pandas as pd

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
# from googleapiclient.errors import HttpError
from google.auth.exceptions import RefreshError

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
        self._connection = None
        return

    def _init_connection(self):
        if self._connection is not None:
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

        self._connection = build('sheets', 'v4', credentials=creds)
        return

    def connection(self):
        self._init_connection()
        return self._connection

class GoogleSheet:
    def __init__(self, interface, config):
        self._interface = interface
        self._id = config["id"]
        self._page = config["page"]
        self._has_header = config.getboolean('header')
        return

    def read(self):
        spreadsheets = self._interface.connection().spreadsheets()

        result = spreadsheets.values().get(spreadsheetId=self._id, range=self._page).execute()

        values = result.get('values', [])

        num_rows = len(values)
        num_columns = max([len(row) for row in values])

        if self._has_header:
            if num_rows < 1:
                raise Exception("Google sheet '%s' page '%s': header expected but no rows" %
                                (self._id, self._page))
            column_names = values[0]

            if len(column_names) != num_columns:
                raise Exception("Google sheet '%s' page '%s': %d columns in header but %d columns in values" %
                                (self._id, self._page, len(column_names), num_columns))


            values = values[1:]
            num_rows -= 1
        else:
            column_names = [_col_num_to_alpha(col_num) for col_num in range(num_columns)]

        columns = {}

        for col_num in range(num_columns):
            column_name = column_names[col_num]
            column_values = [None] * num_rows

            for row_num in range(num_rows):
                row = values[row_num]
                if col_num >= len(row):
                    continue

                value = row[col_num]
                column_values[row_num] = value if value != '' else None

            columns[column_name] = infer_type(column_values)

        df = pd.DataFrame(data=columns)

        return df


    def write(self, df):
        raise Exception('Google sheet writing not supported yet')

