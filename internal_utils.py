
import re
import os
import os.path
import shutil

import numpy as np
import pandas as pd


def to_boolean(s):
    if s is None or isinstance(s, bool):
        return s
    if s.upper() in ['T', 'TRUE']:
        return True
    if s.upper() in ['F', 'FALSE']:
        return False
    raise ValueError()


def project(table, projection):
    if isinstance(table, list):
        return [project(row, projection) for row in table]

    if isinstance(table, dict):
        row = table

        result_dict = {}

        if isinstance(projection, dict):
            for key, value_func in projection.items():
                if value_func is None:
                    value = row[key]
                elif callable(value_func):
                    value = value_func(row)
                elif isinstance(value_func, dict) or isinstance(value_func, list):
                    value = project(row[key], value_func)
                else:
                    value = value_func

                result_dict[key] = value

        elif isinstance(projection, list):
            result_dict = {key: row[key] for key in projection}
        else:
            raise ValueError("Invalid project argument: '%s'" % projection)

        return result_dict

    raise ValueError('Invalid project value type: %s' % type(table))


def infer_type(series):
    """Given a Pandas series of strings, this infers a type that all elements can be converted to:
       integer, float, or timestamp. If such a type is found, a new series of the new type is returned."""

    new_series = None
    converter = None

    _converters = [
        to_boolean,
        np.int64,
        np.float64,
        lambda s: pd.to_datetime(s, utc=True)
    ]

    for i in range(len(series)):
        el = series.iloc[i]

        if el is None:
            continue

        if converter is None:
            for conv in _converters:
                try:
                    new_el = conv(el)
                    converter = conv
                    new_series = pd.Series(index=series.index, dtype=object)
                    new_series.iloc[i] = new_el
                    break
                except ValueError:
                    continue

                if converter is None:
                    # failed to find a type
                    return series
        else:
            try:
                new_series.iloc[i] = converter(el)
            except ValueError:
                if converter == np.int64:
                    converter = np.float64
                    try:
                        new_series.iloc[i] = converter(el)
                    except ValueError:
                        # failed to find a type
                        return series
                else:
                    # failed to find a type
                    return series

    if converter is None:
        return series

    new_series = new_series.convert_dtypes()

    return new_series

def infer_types(df):
    df = df.apply(lambda column: infer_type(column), axis=0)

    return df


def dataframe_duplicate_index_labels(df):
    """Returns the positions of duplicate index labels in a dataframe.
    I'm surprised that Pandas doesn't already offer this."""

    unique_idx = df.index.unique()

    if len(df.index) == len(unique_idx):
        return []

    already_seen_labels = set()

    positions = []

    for i, label in enumerate(df.index):
        if label in already_seen_labels:
            positions.append(i)
        else:
            already_seen_labels.add(label)

    assert len(positions) == len(df.index) - len(unique_idx)

    return positions

def dataframe_drop_rows_at_positions(df, positions):
    """Returns a new dataframe without the rows indicated by the positions.
    I'm surprised pandas doesn't already offer this."""

    new_df_index = [(i not in positions) for i in range(len(df))]

    new_df = df.loc[new_df_index]

    return new_df

def dataframe_ensure_unique_index(df):
    """Makes sure that all dataframe index entries are unique by removing rows that
    have the same index label as a previous row. I'm surprised pandas doesn't already offer this."""

    pos = dataframe_duplicate_index_labels(df)
    return dataframe_drop_rows_at_positions(df, pos)

def get_attrib_or_fail(series, attrib_possible_names):
    for attrib in attrib_possible_names:
        if attrib in series:
            return series[attrib]
    raise Exception('None of the attributes %s are present in series %s' % (attrib_possible_names, series))


def delete_backups(path):
    filename = os.path.basename(path)
    directory = os.path.dirname(path)

    potential_backups = os.listdir(directory)

    backups = [
        backup for backup in potential_backups
        if backup.startswith(filename) and re.fullmatch(r'\.bak(\.[0-9]+)?', backup[len(filename):])
    ]

    if len(backups) == 0:
        return

    for backup in backups:
        os.unlink(os.path.join(directory, backup))

    return

def _backup_name(path, backup_num):
    if backup_num == 0:
        return path + '.bak'
    else:
        return path + '.bak' + '.%d' % backup_num

def _move_backup(path, backup_num, max_backups):
    this_backup = _backup_name(path, backup_num)

    if not os.path.exists(this_backup):
        return

    if backup_num >= max_backups-1:
        # just delete it
        os.unlink(this_backup)
    else:
        # rename it to the next backup
        _move_backup(path, backup_num+1, max_backups)
        next_backup = _backup_name(path, backup_num+1)
        os.rename(this_backup, next_backup)

    return

def back_up_file(path, max_backups):
    if max_backups <= 0:
        return

    if not os.path.exists(path):
        return

    _move_backup(path, 0, max_backups)

    backup = _backup_name(path, 0)
    # os.rename(self._path, backup)
    shutil.copyfile(path, backup)
    return

