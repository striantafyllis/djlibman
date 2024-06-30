
import numpy as np
import pandas as pd


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

def list_of_dicts_to_dict_of_lists(list_of_dicts):
    dict_of_lists = {}

    for i, dct in enumerate(list_of_dicts):
        for key, value in dct.items():
            lst = dict_of_lists.get(key)
            if lst is None:
                lst = [None] * i
                dict_of_lists[key] = lst
            lst.append(value)

    return dict_of_lists

def infer_type(lst):
    """Given a list of strings, this infers a type that fits all elements - integer, float etc.
       If such a type is found, a new list of the new type is returned."""

    new_list = []
    new_list_type = None

    for el in lst:
        if el is None:
            new_list.append(None)
            continue

        if new_list_type is None:
            if el == 'T' or el == 'F':
                new_list_type = np.bool
                new_list.append(np.bool(el == 'T'))
            else:
                for type in [np.int64, np.float64, np.datetime64]:
                    try:
                        new_list.append(type(el))
                        new_list_type = type
                        break
                    except ValueError:
                        continue

                if new_list_type is None:
                    # failed to find a type
                    return lst
        elif new_list_type == np.bool:
            if el == 'T' or el == 'F':
                new_list.append(np.bool(el == 'T'))
            else:
                # failed to find a type
                return lst
        else:
            try:
                new_list.append(new_list_type(el))
            except ValueError:
                if new_list_type == np.int64:
                    new_list_type = np.float64
                    try:
                        new_list.append(new_list_type(el))
                    except ValueError:
                        # failed to find a type
                        return lst
                else:
                    # failed to find a type
                    return lst

    if new_list_type is not None:
        # replace None values in the new list
        for i in range(len(new_list)):
            if new_list[i] is None:
                if new_list_type == np.bool:
                    new_list[i] = np.False_
                elif new_list_type == np.datetime64:
                    new_list[i] = np.datetime64("NaT")
                else:
                    new_list[i] = np.nan

    return new_list

