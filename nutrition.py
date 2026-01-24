
import sys

import pandas as pd
import numpy as np

from spyroslib import google_interface

def issue_error(error_message, continue_on_error=False):
    if continue_on_error:
        print('WARNING: ' + error_message)
    else:
        raise Exception(error_message)


unit_conversion_table = {
    'lb': { 'oz': 16, 'g': 453.592 },
    'floz': { 'ml': 29.5735 },
    'pint': { 'floz': 19.2152, 'pints': 1 },
    'tbsp': { 'tsp': 3, 'ml': 14.7868 },
    'cups': { 'cup': 1, 'floz': 8 },
    'pcs': { 'pc': 1 }
}

def _complete_unit_conversion_table():
    while True:
        starting_unit_conversion_table = dict(unit_conversion_table)

        for unit1, unit1_map in list(unit_conversion_table.items()):
            for unit2, value2 in list(unit1_map.items()):
                unit2_map = unit_conversion_table.get(unit2)

                if unit2_map is None:
                    unit2_map = {}
                    unit_conversion_table[unit2] = unit2_map

                if unit1 not in unit2_map:
                    unit2_map[unit1] = 1.0 / value2

                for unit3, value3 in unit1_map.items():
                    if unit3 == unit2:
                        continue

                    unit3_map = unit_conversion_table.get(unit3)

                    if unit3_map is None:
                        unit3_map = {}
                        unit_conversion_table[unit3] = unit3_map

                    if unit3 not in unit2_map:
                        unit2_map[unit3] = value3 / value2

                    if unit2 not in unit3_map:
                        unit3_map[unit2] = value2 / value3

        if unit_conversion_table == starting_unit_conversion_table:
            break

    return


_complete_unit_conversion_table()


class Nutrition:
    def __init__(self,
                 google_credentials,
                 google_cached_token_file,
                 continue_on_error=False):
        self.nutrition_table = {}
        self.compound_foods = {}
        self.continue_on_error = continue_on_error

        self.google = google_interface.GoogleInterface(
            {
                'credentials': google_credentials,
                'cached_token_file': google_cached_token_file
            }
        )

    def ingest_google_sheet(self, google_sheet_name):
        google_sheet_id = self.google.get_file_id(name=google_sheet_name, type='sheet')

        if google_sheet_id is None:
            issue_error(f"Google sheet '{google_sheet_name}' does not exist",
                        continue_on_error=self.continue_on_error)

        sheets = self.google.get_sheets_in_file(id=google_sheet_id)

        for sheet in sheets:
            self._ingest_sheet(
                google_sheet_name,
                google_sheet_id,
                sheet,
                continue_on_error=self.continue_on_error)

    def fill_in_google_sheet(self, google_sheet_name):
        google_sheet_id = self.google.get_file_id(name=google_sheet_name, type='sheet')

        if google_sheet_id is None:
            issue_error(f"Google sheet '{google_sheet_name}' does not exist",
                        continue_on_error=self.continue_on_error)

        sheets = self.google.get_sheets_in_file(id=google_sheet_id)

        for sheet in sheets:
            if not sheet.endswith('*'):
                self._fill_in_sheet(google_sheet_name,
                                    sheet,
                                    google_sheet_id,
                                    continue_on_error=self.continue_on_error)


    def _process_sheet_common(
            self,
            google_sheet_name,
            sheet,
            google_sheet_id=None,
            continue_on_error=False):
        doc = google_interface.GoogleSheet(
            google_interface=self.google,
            path=google_sheet_name,
            sheet=sheet,
            id=google_sheet_id,
            backups=0,
            header=0,
        )

        contents = doc.read()

        if contents.columns[:3].tolist() != ['Name', 'Quantity', 'Unit']:
            issue_error(f"Google Sheet '{google_sheet_name}'!'{sheet}: "
                        f"unrecognizable columns {contents.columns.tolist()}",
                        continue_on_error=continue_on_error)
            return None

        return doc, contents

    def _ingest_sheet(
            self,
            google_sheet_name,
            google_sheet_id,
            sheet,
            continue_on_error=False):
        print(f'Parsing {google_sheet_name}#{sheet} ...')

        _, contents = self._process_sheet_common(
            google_sheet_name=google_sheet_name,
            google_sheet_id=google_sheet_id,
            sheet=sheet,
            continue_on_error=continue_on_error)

        if contents is None:
            return

        is_source_sheet = sheet.strip().endswith('*')

        current_compound_food = None

        prev_starred_name = None
        prev_starred_unit = None
        for i in range(len(contents)):
            row = contents.iloc[i]

            name = row['Name']

            if pd.isna(name):
                # compound food ingredients end with an empty line
                current_compound_food = None
                prev_starred_name = None
                prev_starred_unit = None
                continue

            name = name.strip().lower()

            is_source_entry = is_source_sheet

            is_continuation = False
            if name.endswith('*'):
                is_source_entry = True
                name = name[:-1]
                if name == '':
                    if prev_starred_name is None:
                        issue_error(f"Google Sheet '{google_sheet_name}'!'{sheet}: "
                                    f"source entry continuation without previous source entry",
                                    continue_on_error=continue_on_error)
                        continue
                    is_continuation = True
                    name = prev_starred_name
                else:
                    prev_starred_name = name
            else:
                prev_starred_name = None

            quantity = row['Quantity'] if not pd.isna(row['Quantity']) else None
            unit = row['Unit'].strip().lower() if not pd.isna(row['Unit']) else None

            # the rather complicated check on whether quantity and/or unit can be empty
            if is_source_entry:
                if is_continuation:
                    if quantity is not None or unit is not None:
                        issue_error(f"Google Sheet '{google_sheet_name}'!'{sheet}: "
                                    f"continuation for source entry '{name}' should not specify "
                                    f"quantity or unit",
                                    continue_on_error=continue_on_error)
                        continue
                else: # is_continuation
                    if quantity is None or unit is None:
                        issue_error(f"Google Sheet '{google_sheet_name}'!'{sheet}: "
                                    f"source entry '{name}' is missing quantity or unit",
                                    continue_on_error=continue_on_error)
                        continue
            else: # is_source_entry
                if (quantity is None) != (unit is None):
                    issue_error(f"Google Sheet '{google_sheet_name}'!'{sheet}: "
                                f"destination entry '{name}' has a quantity but no unit "
                                f"or a unit with no quantity",
                                continue_on_error=continue_on_error)

            values = {
                key: value
                for key, value in row.iloc[3:].items()
                if not pd.isna(value)
            }

            if is_source_entry:
                if len(values) == 0:
                    issue_error(f"Google Sheet '{google_sheet_name}'!'{sheet}: "
                                f"empty source entry '{name}' unit '{unit}'",
                                continue_on_error=continue_on_error)

                values['Quantity'] = quantity

                unit_table = self.nutrition_table.get(name)

                if unit_table is None:
                    unit_table = {}
                    self.nutrition_table[name] = unit_table

                if is_continuation:
                    if prev_starred_unit is None or prev_starred_unit not in unit_table:
                        # this should never happen if the code doesn't have a bug
                        assert False

                    prev_starred_entry = unit_table[prev_starred_unit]

                    for key, value in values.items():
                        if value is None:
                            continue
                        prev_starred_entry[key] += value
                else: # is_continuation
                    if unit in unit_table:
                        issue_error(f"Google Sheet '{google_sheet_name}'!'{sheet}: "
                                    f"duplicate source entry '{name}' unit '{unit}'",
                                    continue_on_error=continue_on_error)
                        continue

                    unit_table[unit] = values
                    prev_starred_unit = unit

                if not is_source_sheet and not is_continuation and current_compound_food is not None:
                    current_compound_food['Ingredients'].append({
                        'Name': name,
                        'Quantity': quantity,
                        'Unit': unit
                    })

                continue
            else: # is_source_entry
                if current_compound_food is None:
                    if name in self.compound_foods:
                        issue_error(f"Google Sheet '{google_sheet_name}'!'{sheet}: "
                                    f"duplicate compound entry '{name}'",
                                    continue_on_error=continue_on_error)
                        continue

                    current_compound_food = {'Name': name}
                    if not pd.isna(quantity):
                        current_compound_food['Quantity'] = quantity
                        current_compound_food['Unit'] = unit

                    self.compound_foods[name] = current_compound_food

                    current_compound_food['Ingredients'] = []
                else:
                    current_compound_food['Ingredients'].append({
                        'Name': name,
                        'Quantity': quantity,
                        'Unit': unit
                    })
        return


    def calculate_nutrition_info(self, food, quantity=None, unit=None, convert_units=True):
        if quantity is not None and unit is None:
            raise ValueError('Unit must be present if quantity is present')

        food = food.lower()

        # first, look in the nutrition table
        food_unit_info = self.nutrition_table.get(food.lower())

        if food_unit_info is not None:
            if quantity is None or unit is None:
                raise ValueError(f"Food {food} is in the nutrition table; quantity and unit must be specified.")

            if unit in food_unit_info:
                food_info = food_unit_info[unit]
                food_info_quantity = food_info['Quantity']

                nutrition_info = {}

                for key, value in food_info.items():
                    if key == 'Quantity':
                        continue

                    nutrition_info[key] = value * quantity / food_info_quantity

                return nutrition_info

            elif convert_units:
                # look for unit conversions
                other_units = unit_conversion_table.get(unit)

                if other_units is not None:
                    for other_unit, conversion_rate in other_units.items():
                        result = self.calculate_nutrition_info(
                            food,
                            quantity=quantity * conversion_rate,
                            unit=other_unit,
                            convert_units=False
                        )

                        if result is not None:
                            return result

                raise ValueError(f"Food {food}: cannot convert unit {unit} to any of the available units in the nutrition table")
            else:
                return None

        # not found in conversion table; look in compound food table

        compound_food_info = self.compound_foods.get(food.lower())

        if compound_food_info is None:
            raise ValueError(f"Food '{food}' not found")

        food_info_quantity = compound_food_info.get('Quantity')
        food_info_unit = compound_food_info.get('Unit')

        if food_info_quantity is not None and quantity is None:
            raise ValueError(f"Food {food} is a measurable compound food; quantity and unit must be specified.")

        if food_info_unit is None:
            conversion_rate = 1.0
        elif food_info_unit == unit:
            conversion_rate = quantity / food_info_quantity
        else:
            unit_conversion_rates = unit_conversion_table.get(unit)

            if unit_conversion_rates is None or food_info_unit not in unit_conversion_rates:
                raise ValueError(f"Food '{food}': cannot convert unit {unit} to compound food table unit {food_info_unit}")

            conversion_rate = unit_conversion_rates[food_info_unit] * quantity / food_info_quantity

        nutrition_info = {}

        for ingredient in compound_food_info['Ingredients']:
            ingredient_name = ingredient['Name']
            ingredient_quantity = ingredient.get('Quantity')
            ingredient_unit = ingredient.get('Unit')

            ingredient_nutrition_info = self.calculate_nutrition_info(
                ingredient_name,
                ingredient_quantity,
                ingredient_unit,
                convert_units=True
            )

            for key, value in ingredient_nutrition_info.items():
                value *= conversion_rate

                if key in nutrition_info:
                    nutrition_info[key] += value
                else:
                    nutrition_info[key] = value

        return nutrition_info

    def _fill_in_sheet(
            self,
            google_sheet_name,
            sheet,
            google_sheet_id,
            continue_on_error=False):
        if sheet.endswith('*'):
            # source sheet; nothing to fill in
            return

        print(f'Filling in {google_sheet_name}#{sheet} ...')

        doc, contents = self._process_sheet_common(
            google_sheet_name=google_sheet_name,
            sheet=sheet,
            google_sheet_id=google_sheet_id,
            continue_on_error=continue_on_error)

        for row_idx in range(len(contents)):
            row = contents.iloc[row_idx]

            name = row['Name']

            if pd.isna(name):
                continue

            name = name.strip().lower()

            if name.endswith('*'):
                # source row; nothing to fill in
                continue

            quantity = row['Quantity'] if not pd.isna(row['Quantity']) else None
            unit = row['Unit'].strip().lower() if not pd.isna(row['Unit']) else None

            nutrition_info = self.calculate_nutrition_info(name, quantity, unit)

            for key, value in nutrition_info.items():
                if key not in contents.columns:
                    continue

                # fix an annoying problem...
                if isinstance(value, float) and not isinstance(value, int):
                    if not np.issubdtype(contents[key].dtype.type, np.float64):
                        contents[key] = contents[key].astype(pd.Float64Dtype(), copy=False)

                col_idx = contents.columns.get_loc(key)

                contents.iloc[row_idx, col_idx] = value

        doc.write(contents)

        return


google_sheets_to_read = [
    'Nutrition Info',
]
google_sheets_to_fill_in = [
    'Food Tracking 2026-01',
]

google_credentials =  '/Users/spyros/google_credentials_music_library_management.json'
google_cached_token_file = 'google_cached_token.json'

def main():
    nutrition = Nutrition(
        google_credentials,
        google_cached_token_file,
        continue_on_error=False
    )

    for google_sheet in google_sheets_to_read + google_sheets_to_fill_in:
        nutrition.ingest_google_sheet(google_sheet)

    for google_sheet in google_sheets_to_fill_in:
        nutrition.fill_in_google_sheet(google_sheet)

    return

if __name__ == '__main__':
    main()
    sys.exit(0)
