"""
Contains functions that are kind of like one-off scripts to be run on the common infrastructure -
the Google sheet, the Spotify service, the YouTube service etc.
Each script function takes as arguments:
- the Rekordbox state
- the Google sheet
"""

import google_sheet
from OLD.utils import *
import library_organizer

valid_classes = ['A', 'B', 'C', 'X']

def query_mix_13(
        rekordbox_state: library_organizer.RekordboxState,
        sheet: google_sheet.GoogleSheet
):
    # playlist: exuberance
    # class: A or B
    # BPM: >= 110

    tracklist = []

    for track in sheet:
        track_playlists = track.get('Playlists')
        if track_playlists is None or track_playlists == '':
            print('WARNING: Track with no playlists: %' % track)
            continue

        track_playlists = track_playlists.split(',')
        track_playlists = [p.strip().upper() for p in track_playlists]

        if 'EXUBERANCE' not in track_playlists:
            continue

        track_class = track.get('Class')
        if track_class not in ('A', 'B'):
            continue

        track_bpm = track.get('BPM')
        if track_bpm < 110:
            continue

        tracklist.append(track)

    return tracklist


def query_class(
        rekordbox_state: library_organizer.RekordboxState,
        sheet: google_sheet.GoogleSheet,
        classes,
        min_bpm=None,
        max_bpm=None
):
    classes = classes.upper()
    min_bpm = float(min_bpm) if min_bpm is not None else None
    max_bpm = float(max_bpm) if max_bpm is not None else None

    tracklist = []

    for track in sheet:
        track_class = track.get('Class')
        if track_class not in valid_classes:
            print("WARNING: Track with invalid class '%s': %s" % (track_class, track))
            continue

        track_bpm = track.get('BPM')

        if (track_class in classes and
                (min_bpm is None or track_bpm >= min_bpm) and
                (max_bpm is None or track_bpm <= max_bpm)):
            tracklist.append(track)

    return tracklist



def query_flavors(
        rekordbox_state: library_organizer.RekordboxState,
        sheet: google_sheet.GoogleSheet,
        flavor,
        min_bpm,
        max_bpm
):
    flavor = flavor.upper()
    min_bpm = float(min_bpm)
    max_bpm = float(max_bpm)

    tracklist = []

    for track in sheet:
        track_flavors = track.get('Flavors')
        if track_flavors is None or track_flavors == '':
            # print('WARNING: Track with no flavors: %' % track)
            continue

        track_flavors = track_flavors.split(',')
        track_flavors = [p.strip().upper() for p in track_flavors]

        track_bpm = track.get('BPM')

        if flavor in track_flavors and track_bpm >= min_bpm and track_bpm <= max_bpm:
            tracklist.append(track)

    return tracklist

def query_playlists(
        rekordbox_state: library_organizer.RekordboxState,
        sheet: google_sheet.GoogleSheet,
        *playlists
):
    playlists = [p.upper() for p in playlists]

    tracklist = []

    for track in sheet:
        track_playlists = track.get('Playlists')
        if track_playlists is None or track_playlists == '':
            print('WARNING: Track with no playlists: %' % track)
            continue

        track_playlists = track_playlists.split(',')
        track_playlists = [p.strip().upper() for p in track_playlists]

        is_in_playlists = True
        for p in playlists:
            is_in_playlists &= p in track_playlists

        if is_in_playlists:
            tracklist.append(track)

    return tracklist



def populate_instruments(
        batch_mode: bool,
        rekordbox_state: library_organizer.RekordboxState,
        sheet: google_sheet.GoogleSheet
):
    num_changed_fields = 0

    for track in sheet.tracks:
        instruments = []

        if track.attributes['Strings']:
            instruments.append('Strings')
        if track.attributes['Wind']:
            instruments.append('Wind')

        if len(instruments) > 0:
            track.attributes['Instruments'] = instruments
            num_changed_fields += 1

    if num_changed_fields > 0:
        choice = get_user_choice(batch_mode, 'Write back %d Instruments fields?' % num_changed_fields)
        if choice == 'yes':
            sheet.write_back()
    return

def set_danceable(
        rekordbox_state: library_organizer.RekordboxState,
        sheet: google_sheet.GoogleSheet
):
    num_changed_fields = 0
    for track in sheet.tracks:
        track.attributes['Danceable'] = track.attributes['Motivates Dancing'] or track.attributes['Sustains Dancing']

    sheet.write_back()
    return


def merge_genres_and_flavors(
        batch_mode: bool,
        rekordbox_state: library_organizer.RekordboxState,
        sheet: google_sheet.GoogleSheet
):
    num_changed_fields = 0
    for track in sheet.tracks:
        genres = track.attributes.get('Genres')
        flavors = track.attributes.get('Flavors')

        if genres is None:
            continue
        genres = genres.split(', ')

        if flavors is None:
            flavors = []
        else:
            flavors = flavors.split(', ')

        if 'Organic' in genres:
            genres.remove('Organic')

        if genres == []:
            continue

        flavors = genres + flavors

        track.attributes['Flavors'] = flavors
        num_changed_fields += 1

    if num_changed_fields > 0:
        choice = get_user_choice(batch_mode, 'Write %d changed flavors?' % num_changed_fields)
        if choice == 'yes':
            sheet.write_back()
            print('Wrote %d changed flavors' % num_changed_fields)

    return

def fill_in_flavors(
        batch_mode: bool,
        rekordbox_state: library_organizer.RekordboxState,
        sheet: google_sheet.GoogleSheet
):
    flavor_attributes = [
        attribute for attribute in sheet.attribute_to_col_num.keys()
        if attribute.endswith(' Flavor')
    ]

    flavor_attributes.sort(key=lambda x: sheet.attribute_to_col_num[x])

    num_changed_fields = 0
    for track in sheet.tracks:
        if track.attributes.get('Flavors') is not None:
            continue

        flavors = []

        for flavor_attribute in flavor_attributes:
            if track.attributes.get(flavor_attribute) is True:
                flavors.append(flavor_attribute[:-7])

        if len(flavors) > 0:
            track.attributes['Flavors'] = flavors
            track._dirty_attributes.add('Flavors')
            num_changed_fields += 1

    choice = get_user_choice(batch_mode, 'Filled in %d flavors; write?' % num_changed_fields)
    if choice == 'yes':
        sheet.write_back()
        print('Wrote %d flavors' % num_changed_fields)

    return


def fill_in_genres(
        batch_mode: bool,
        rekordbox_state: library_organizer.RekordboxState,
        sheet: google_sheet.GoogleSheet
):
    num_changed_fields = 0
    for track in sheet.tracks:
        if track.attributes.get('Genres') is not None:
            continue

        genres = []

        for genre_attrib in ['Organic', 'Tech', 'Acoustic', 'Songy song']:
            if track.attributes.get(genre_attrib) is True:
                if genre_attrib == 'Songy song':
                    genre_attrib = 'Songy'
                genres.append(genre_attrib)

        if len(genres) > 0:
            track.attributes['Genres'] = genres
            track._dirty_attributes.add('Genres')
            num_changed_fields += 1

    choice = get_user_choice(batch_mode, 'Filled in %d genres; write?' % num_changed_fields)
    if choice == 'yes':
        sheet.write_back()
        print('Wrote %d genres' % num_changed_fields)

    return



def fix_boolean_attributes(
        batch_mode,
        rekordbox_state: library_organizer.RekordboxState,
        sheet: google_sheet.GoogleSheet
):
    """Turns old-style Boolean attributes ('x' or None) to new-style Boolean attributes (T or F)"""

    # detect boolean attributes
    boolean_attributes = list(sheet.attribute_to_col_num.keys())
    boolean_attributes.sort(key=lambda attr: sheet.attribute_to_col_num[attr])
    last_set_row = 0

    for track in sheet.tracks:
        for attribute, value in track.attributes.items():
            if attribute not in boolean_attributes:
                continue
            if value is True or value == 'x':
                last_set_row = track.id
            elif value is not None:
                boolean_attributes.remove(attribute)

    print('Detected Boolean attributes:')
    for attribute in boolean_attributes:
        print("    '%s'" % attribute)
    print('Detected last set row: %d' % last_set_row)

    reply = get_user_choice(batch_mode, 'Fix?')

    if reply == 'no':
        print('Aborting')
        return

    num_changed_fields = 0
    for track in sheet.tracks:
        if track.id > last_set_row:
            break

        for attribute in boolean_attributes:
            track.attributes[attribute] = 'T' if track.attributes[attribute] else 'F'
            track._dirty_attributes.add(attribute)
            num_changed_fields += 1

    print('Writing back %d changed fields' % num_changed_fields)
    sheet.write_back()

    return




