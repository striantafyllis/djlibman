

import pandas as pd
import numpy as np

# needed for calls to eval() on csvs etc.
from pandas import Timestamp

from general_utils import *

# some basic conversion functions

def _is_empty(value):
    return pd.isna(value) or value == ''

def to_boolean(value):
    if _is_empty(value):
        return pd.NA
    if value.upper() in ['T', 'TRUE']:
        return True
    if value.upper() in ['F', 'FALSE']:
        return False
    raise Exception("Illegal value in boolean column: '%s'" % value)

def from_boolean(value):
    if pd.isna(value):
        return value
    return 'T' if value == True else 'F'

def to_list(value):
    if _is_empty(value):
        return value
    return [el.strip() for el in value.split(',')]

def from_list(value):
    if isinstance(value, list):
        return ', '.join(value)

class FileDoc:
    def __init__(self,
                 path,
                 backups = 0,
                 header = 0,
                 index_column = '_FIRST_COLUMN',
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
                if column_name in self._contents.columns:
                    # I could do this with pd.Series.apply() but I'll do it the slow and
                    # laborious way so we can have proper error reporting.
                    # As to why I form the entire column first before assignment instead of assigning
                    # one element at a time:
                    # Pandas has a bug (or a feature?) that doesn't let me set a single element to a list
                    # value. It thinks the list value is a list of values that should be put in a
                    # list of cells, and I get this exception from indexing.py:
                    # ValueError: Must have equal len keys and value when setting with an iterable

                    column = self._contents[column_name]

                    new_column = []
                    for i in range(len(column)):
                        try:
                            orig_value = column.iloc[i]
                            value = converter(orig_value)
                            new_column.append(value)
                        except Exception as e:
                            raise ValueError(
                                f"Document {self._path}, row {i+1}, column {column_name}: "
                                f"conversion of value '{orig_value}' failed "
                                f"with error: {e}"
                            )

                    self._contents[column_name] = new_column

        if self._index_column is not None:
            if self._index_column == '_FIRST_COLUMN' and len(self._contents.columns) > 0:
                self._contents.set_index(self._contents.columns[0], drop=False, inplace=True)
            else:
                self._contents.set_index(self._index_column, drop=False, inplace=True)

        return

    def _raw_read(self):
        raise Exception('Not implemented')

    def _raw_write(self, df):
        raise Exception('Not implemented')

    def exists(self):
        return os.path.exists(self._path)

    def delete(self):
        os.remove(self._path)
        return

    def getmtime(self):
        if not self.exists():
            return None
        return os.path.getmtime(self._path)

    def read(self, force=False):
        if force or self._last_read_time is None or os.path.getmtime(self._path) > self._last_read_time:
            self._parse()

        return self._contents

    def write(self, df):
        back_up_file(self._path, self._backups)

        df2 = pd.DataFrame(df)
        for column_name, deconverter in self._deconverters.items():
            if column_name in self._contents.columns:
                df2[column_name] = df2[column_name].apply(deconverter)

        self._raw_write(df2)
        return

    def delete_backups(self):
        delete_backups(self._path)
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
                    try:
                        raw.iat[row, col] = eval(el)
                    except SyntaxError:
                        pass
                    except NameError:
                        pass

        return raw

    def _raw_write(self, df):
        df.to_csv(self._path,
                  header=(self._header is not None),
                  index=False)
        return


