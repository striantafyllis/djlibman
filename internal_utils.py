
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
