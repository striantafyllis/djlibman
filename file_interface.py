
import os
import os.path
import re
import shutil

import pandas as pd
import numpy as np

# needed for calls to eval() on csvs etc.
from pandas import Timestamp

from utils import *


# some basic conversion functions

def _is_empty(value):
    return pd.isna(value) or value == ''

def to_boolean(value):
    if _is_empty(value):
        return False
    if value.upper() in ['T', 'TRUE']:
        return True
    if value.upper() in ['F', 'FALSE']:
        return False
    raise Exception("Illegal value in boolean column: '%s'" % value)

def from_boolean(value):
    return 'T' if value else 'F'

def to_list(value):
    if _is_empty(value):
        return []
    return [el.strip() for el in value.split(',')]

def from_list(value):
    if isinstance(value, list):
        return ', '.join(value)

class FileDoc:
    def __init__(self,
                 path,
                 backups = 0,
                 header = None,
                 index_column = None,
                 list_columns = [],
                 boolean_columns = [],
                 datetime_columns = [],
                 datetime_format = None
                 ):
        self._path = path
        self._backups = backups
        self._header = header
        self._index_column = index_column
        self._apply_converters = True

        self._converters = {}
        self._deconverters = {}

        for column in list_columns:
            self._converters[column] = to_list
            self._deconverters[column] = from_list

        for column in boolean_columns:
            self._converters[column] = to_boolean
            self._deconverters[column] = from_boolean

        for column in datetime_columns:
            self._converters[column] = lambda x: pd.to_datetime(x, utc=True, format=datetime_format)
            if datetime_format is not None:
                self._deconverters[column] = lambda x: x.strftime(format=datetime_format)

        self._last_read_time = None
        self._contents = None
        return

    def _parse(self):
        self._last_read_time = os.path.getmtime(self._path)
        self._contents = self._raw_read()

        if self._apply_converters:
            for column_name, converter in self._converters.items():
                self._contents[column_name] = self._contents[column_name].apply(converter)

        if self._index_column is not None:
            self._contents = self._contents.set_index(self._contents[self._index_column])

        return

    def _raw_read(self):
        raise Exception('Not implemented')

    def _raw_write(self, df):
        raise Exception('Not implemented')

    def read(self, force=False):
        if force or self._last_read_time is None or os.path.getmtime(self._path) > self._last_read_time:
            self._parse()

        return self._contents

    def delete_backups(self):
        filename = os.path.basename(self._path)
        directory = os.path.dirname(self._path)

        potential_backups = os.listdir(directory)

        backups = [
            backup for backup in potential_backups
            if backup.startswith(filename) and re.fullmatch(r'\.bak(\.[0-9]+)?', backup[len(filename):])
        ]

        if len(backups) == 0:
            return

        print('Potential backup files for doc %s: %s' % (self._path, backups))
        choice = get_user_choice('Delete?')

        if choice == 'yes':
            for backup in backups:
                os.unlink(os.path.join(directory, backup))

        return

    def _backup_name(self, backup_num):
        if backup_num == 0:
            return self._path + '.bak'
        else:
            return self._path + '.bak' + '.%d' % backup_num

    def _move_backup(self, backup_num):
        this_backup = self._backup_name(backup_num)

        if not os.path.exists(this_backup):
            return

        if backup_num >= self._backups-1:
            # just delete it
            os.unlink(this_backup)
        else:
            # rename it to the next backup
            self._move_backup(backup_num+1)
            next_backup = self._backup_name(backup_num+1)
            os.rename(this_backup, next_backup)

        return

    def _backup_current(self):
        if self._backups <= 0:
            return

        if not os.path.exists(self._path):
            return

        self._move_backup(0)

        backup = self._backup_name(0)
        # os.rename(self._path, backup)
        shutil.copyfile(self._path, backup)
        return

    def write(self, df):
        self._backup_current()

        df2 = pd.DataFrame(df)
        for column_name, deconverter in self._deconverters.items():
            df2[column_name] = df2[column_name].apply(deconverter)

        self._raw_write(df2)
        return


class ExcelSheet(FileDoc):
    def __init__(self, path, sheet=0, **kwargs):
        super(ExcelSheet, self).__init__(path, **kwargs)
        self._sheet = sheet

    def _raw_read(self):
        raw = pd.read_excel(
            io = self._path,
            sheet_name = self._sheet,
            header = self._header
        )

        # this is necessary because int columns will show up as floats
        raw = raw.apply(lambda column: column.convert_dtypes(), axis=0)

        return raw

    def _raw_write(self, df):
        excel_writer = pd.ExcelWriter(
            engine='openpyxl',
            path=self._path,
            mode='a',
            if_sheet_exists='overlay',
            engine_kwargs={ 'rich_text': True }
        )

        # remove timezone info from timestamps because Excel doesn't support timezones
        df2 = df.map(lambda x: x.to_datetime64() if isinstance(x, pd.Timestamp) else x)

        df2.to_excel(
            excel_writer=excel_writer,
            sheet_name=self._sheet,
            header=True,
            index=False
        )
        excel_writer.close()
        return

class CsvFile(FileDoc):
    def __init__(self, path, **kwargs):
        super(CsvFile, self).__init__(path, **kwargs)
        self._apply_converters = False

    def _raw_read(self):
        raw = pd.read_csv(
            self._path,
            header = self._header,
            converters = self._converters
        )

        # Pandas can save nested Python objects, but then it reads them back as strings.
        # This code corrects that mess automatically as much as possible. Declaring converters also helps.

        rows, cols = raw.shape

        for row in range(rows):
            for col in range(cols):
                el = raw.iat[row,col]
                if isinstance(el, str) and len(el) >= 2 and el[0] in ['[', '{']:
                    # try:
                        raw.iat[row, col] = eval(el)
                    # except SyntaxError:
                    #     pass

        return raw

    def _raw_write(self, df):
        df.to_csv(self._path,
                  header=(self._header is not None),
                  index=False)
        return

