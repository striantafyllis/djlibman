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
    def __init__(self, interface, id, page, has_header=False, column_types = {}):
        self._interface = interface
        self._id = id
        self._page = page
        self._has_header = has_header
        self._column_types = column_types
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

        # a bit of a mess here because config column names come out in lowercase
        column_names_lower = [column_name.lower() for column_name in column_names]
        for column_name in self._column_types:
            if column_name not in column_names_lower:
                raise Exception("Google sheet '%s' page '%s': column '%s' has a declared type but is not present in header" %
                                (self._id, self._page, column_name))

        columns = {}

        for col_num in range(num_columns):
            column_name = column_names[col_num]
            column_type = self._column_types.get(column_name.lower())
            column_values = [None] * num_rows

            for row_num in range(num_rows):
                row = values[row_num]
                if col_num >= len(row):
                    continue

                value = row[col_num]

                if column_type is not None:
                    if column_type == list:
                        value = [s.strip() for s in value.split(',')]
                    else:
                        try:
                            value = column_type(value)
                        except ValueError:
                            raise Exception(
                                "Google sheet '%s' page '%s' column %s row %d: value '%s'; expected %s" %
                                (self._id, self._page, column_name, row_num, value, column_type))

                column_values[row_num] = value

            columns[column_name] = pd.Series(column_values)

        df = pd.DataFrame(data=columns)

        return df


    def write(self, df):
        raise Exception('Google sheet writing not supported yet')

