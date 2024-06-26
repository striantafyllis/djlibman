
import pandas as pd
import numpy as np
import os
import os.path

import ast

# some basic conversion functions

def _is_empty(value):
    # this is needed because pandas.read_excel() turns empty cells to NaN
    return value is None or value == '' or (isinstance(value, float) and np.isnan(value))

def to_boolean(value):
    if _is_empty(value):
        return False
    if value.upper() in ['T', 'TRUE']:
        return True
    if value.upper() in ['F', 'FALSE']:
        return False
    raise Exception("Illegal value in boolean column: '%s'" % value)

def to_list(value):
    if _is_empty(value):
        return []
    return [el.strip() for el in value.split(',')]

def to_date(value):
    if _is_empty(value):
        return np.datetime64("NaT")
    return np.datetime64(value)


class FileDoc:
    def __init__(self, config):
        self._path = config['path']
        if 'header' in config:
            self._header = config.getint('header')
        else:
            self._header = None

        self._converters = {}

        if 'list_columns' in config:
            list_columns = ast.literal_eval(config['list_columns'])

            for column in list_columns:
                self._converters[column] = to_list

        if 'boolean_columns' in config:
            boolean_columns = ast.literal_eval(config['boolean_columns'])

            for column in boolean_columns:
                self._converters[column] = to_boolean

        if 'date_columns' in config:
            date_columns = ast.literal_eval(config['date_columns'])

            for column in date_columns:
                self._converters[column] = to_date

        self._last_read_time = None
        self._contents = None
        return

    def _parse(self):
        self._last_read_time = os.path.getmtime(self._path)
        self._contents = self._raw_read()

        for column_name, converter in self._converters.items():
            self._contents[column_name] = self._contents[column_name].apply(converter)

        return

    def _raw_read(self):
        raise Exception('Not implemented')

    def get(self):
        if self._last_read_time is None or os.path.getmtime(self._rekordbox_xml) > self._last_read_time:
            self._parse()

        return self._contents


class ExcelSheet(FileDoc):
    def __init__(self, config):
        super(ExcelSheet, self).__init__(config)
        assert config['type'] == 'excel'

        if 'sheet' in config:
            self._sheet = config['sheet']
        else:
            self._sheet = 0

    def _raw_read(self):
        raw = pd.read_excel(
            io = self._path,
            sheet_name = self._sheet,
            header = self._header
        )

        return raw


