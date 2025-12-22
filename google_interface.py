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

    def get_file_id(self, name, type=None):
        files = self.get_files(type=type, name=name)

        if len(files) == 0:
            return None
        elif len(files) == 1:
            return files[0]['id']
        else:
            raise ValueError(f"More than one Google docs match name '{name}'" +
                             f' and type {type}' if type is not None else '')

    def get_sheets_in_file(self, name=None, id=None):
        if id is None:
            if name is not None:
                id = self.get_file_id(name=name, type='sheet')
            else:
                raise ValueError('At least one of name or id should be specified')

        if id is None:
            raise ValueError(f"Google Sheet '{name}' does not exist")

        properties = self.sheets_connection().spreadsheets().get(
            spreadsheetId = id,
            fields = 'sheets.properties'
        ).execute()

        return [prop['properties']['title'] for prop in properties['sheets']]

class Change:
    '''A class that represents changes of ranges of values during write.
       top_row and left_col are zero-based integers.
       values has to be a rectangular nonempty list of lists'''

    def __init__(self, top_row, left_col, values):
        self._top_row = top_row
        self._left_col = left_col
        self._values = values

        if not isinstance(values, list):
            raise ValueError('values has to be a list')
        if len(values) == 0:
            raise ValueError('values has to be a nonempty list')

        self._num_rows = len(values)

        self._num_cols = None
        for row in values:
            if not isinstance(row, list):
                raise ValueError('values has to be a list of lists')
            if len(row) == 0:
                raise ValueError('values has to be  list of nonempty lists')

            if self._num_cols is None:
                self._num_cols = len(row)
            elif self._num_rows != len(row):
                raise ValueError(f'values contains rows of unequal length: {self._num_cols} vs. {len(row)}')

        return

    def try_to_merge(self, other):
        '''Merges two changes if possible. If the merge is successful, this returns
        the merged change. Otherwise returns None.'''

        # Two changes can be merged if they have the same columns
        # and are adjacent row-wise, or v.v.

        if self._top_row == other._top_row and self._num_rows == other._num_rows:
            if self._left_col < other._left_col:
                left = self
                right = other
            else:
                left = other
                right = self

            if left._left_col + left._num_cols == right._left_col:
                new_right_col = left._left_col
                new_top_row = left._top_row
                new_values = [
                    left._values[row_num] + right._values[row_num]
                    for row_num in range(left._num_rows)
                ]

                return Change(new_top_row, new_right_col, new_values)
            else:
                return None

        if self._left_col == other._left_col and self._num_cols == other._num_cols:
            if self._top_row < other._top_row:
                upper = self
                lower = other
            else:
                upper = other
                lower = self

            if upper._top_row + upper._num_rows == lower._top_row:
                new_top_row = upper._top_row
                new_right_col = upper._left_col
                new_values = upper._values + lower._values

                return Change(new_top_row, new_right_col, new_values)
            else:
                return None

        return None

    def get_range(self):
        bottom_row = self._top_row + self._num_rows
        right_col = self._left_col + self._num_cols

        top_left_corner = _col_num_to_alpha(self._left_col) + f'{self._top_row+1}'
        bottom_right_corner= _col_num_to_alpha(right_col) + f'{bottom_row+1}'

        return f'{top_left_corner}:{bottom_right_corner}'

    def get_values(self):
        return self._values


class GoogleSheet(FileDoc):
    def __init__(self,
                 google_interface,
                 path,
                 sheet,
                 id=None,
                 **kwargs
                 ):
        super(GoogleSheet, self).__init__(path, **kwargs)
        self._interface = google_interface
        self._sheet = sheet
        self._id = id

        self._read_value_converters = [ np.int64, np.float64 ]
        # if convert_datetime:
        #     self._read_value_converters.append(lambda s: pd.to_datetime(s, utc=True))

        return

    def _convert_read_value(self, value):
        if value is None or value == '':
            return None

        for conv in self._read_value_converters:
            try:
                new_value = conv(value)
                return new_value
            except ValueError:
                continue

        return value

    def _init_id(self):
        if self._id is not None:
            return

        self._id = self._interface.get_file_id(type='sheet', name=self._path)

        if self._id is None:
            self._id = '_DOESNT_EXIST'

        return

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

    def _read_as_list(self):
        self._init_id()

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
        # Additionally, if columns are missing from the end, the Google row will be shorter.

        conv_values = [
            [self._convert_read_value(row[i]) if i < len(row) else None for i in range(len(columns))]
            for row in values
        ]

        return columns, conv_values


    def _raw_read(self):
        columns, conv_values = self._read_as_list()

        df = pd.DataFrame(data=conv_values, columns=columns)

        # this is necessary because int columns will show up as floats
        df2 = df.apply(lambda column: column.convert_dtypes(), axis=0)

        return df2

    @classmethod
    def _get_for_write(self, two_dim_array, row, col):
        '''
        Gets a value from a two-dimensional list of lists that is either values read or
        values about to be written to a Google Sheet. All the following values:
        - values out of boundaries
        - None
        - Pandas NA
        - Empty lists
        are converted to the empty string (''). This is just what we need for writing
        to a Google Sheet.

        Indices i and j are zero-based.
        '''

        if row >= len(two_dim_array):
            return ''

        rowvals = two_dim_array[row]

        if col >= len(rowvals):
            return ''

        val = rowvals[col]

        if val is None:
            return ''
        if isinstance(val, list):
            if len(val) == 0:
                return ''
        elif pd.isna(val):
            return ''

        return val


    def _raw_write(self, df):
        self._init_id()

        new_values = [df.columns.tolist()] + df.values.tolist()

        # see what's already there
        old_cols, old_vals = self._read_as_list()
        old_values = [old_cols] + old_vals

        # see what's changed

        changes = []

        for row in max(len(old_values), len(new_values)):
            for col in max(len(old_values[row]), len(new_values[row])):
                old_val = self._get_for_write(old_values, row, col)
                new_val = self._get_for_write(new_values, row, col)

                if old_val != new_val:
                    changes.append(Change(row, col, [[new_val]]))

        # try to merge changes. This algorithm tries to merge changes vertically
        # as much as possible and then horizontally as much as possible.
        # It is not guaranteed to come up with the minimum number of changes,
        # but it should do well enough most of the time.

        for direction in ['horizontal', 'vertical']:
            if direction == 'horizontal':
                changes.sort(lambda c: c._left_col)
            else:
                changes.sort(lambda c: c._top_row)

            idx = 0
            while idx < len(changes)-1:
                merged_change = changes[idx].try_to_merge(changes[idx+1])
                if merged_change is not None:
                    changes = changes[:idx] + [merged_change] + changes[idx+2:]
                else:
                    idx += 1


        data = []

        for change in changes:
            data.append({
                'range': change.get_range(),
                'majorDimension': 'ROWS',
                'values': change.get_values()
            })

        body = {
            'valueInputOption': 'USER_ENTERED',
            'includeValuesInResponse': False,
            'data': data
        }

        self._interface.sheets_connection().spreadsheets().values().batchUpdate(
            spreadsheetId=self._id,
            body = body
        ).execute()

        # self._interface.sheets_connection().spreadsheets().values().update(
        #     spreadsheetId=self._id, range=self._sheet,
        #     valueInputOption='USER_ENTERED',
        #     includeValuesInResponse=False,
        #     body={
        #         'range': self._sheet,
        #         'values': new_values,
        #         'majorDimension': 'ROWS'
        #     }
        # ).execute()


        return



    def delete(self):
        raise Exception('Deleting Google sheets not supported')
