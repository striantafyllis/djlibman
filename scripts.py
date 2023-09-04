"""
Contains functions that are kind of like one-off scripts to be run on the common infrastructure -
the Google sheet, the Spotify service, the YouTube service etc.
Each script function takes as arguments:
- the Rekordbox state
- the Google sheet
"""

import rekordbox
import google_sheet
from streaming_service import StreamingService
from spotify_service import SpotifyService
from youtube_service import YouTubeService
from utils import *
import library_organizer


def query_playlists(
        rekordbox_state: library_organizer.RekordboxState,
        sheet: google_sheet.Sheet,
        *playlists
):
    playlists = [p.upper() for p in playlists]

    tracklist = []

    for track in rekordbox_state.main_library.tracks:
        # TODO this should not be necessary if sanity checks have passed
        if track.track_info is None:
            continue

        track_playlists = track.track_info.attributes['Playlists']
        if track_playlists is None or track_playlists == '':
            print('WARNING: Track with no playlists: %s \u2013 %s' % (track.artist_orig, track.title))
            continue

        track_playlists = track_playlists.split(',')
        track_playlists = [p.strip().upper() for p in track_playlists]

        is_in_playlists = True
        for p in playlists:
            is_in_playlists &= p in track_playlists

        if is_in_playlists:
            tracklist.append(track)

    return tracklist



def populate_riffs(
        rekordbox_state: library_organizer.RekordboxState,
        sheet: google_sheet.Sheet
):
    riff_attributes = [
        attribute for attribute in sheet.attribute_to_col_num.keys()
        if attribute.endswith(' Solo')
    ]

    riff_attributes.sort(key=lambda x: sheet.attribute_to_col_num[x])

    num_changed_fields = 0
    for track in sheet.tracks:
        if track.attributes.get('Riffs') is not None:
            continue

        riffs = []

        for riff_attribute in riff_attributes:
            if track.attributes.get(riff_attribute) is True:
                riffs.append(riff_attribute[:-5])

        if len(riffs) > 0:
            track.attributes['Riffs'] = riffs
            track.dirty_fields.add('Riffs')
            num_changed_fields += 1

    choice = get_user_choice('Filled in %d riffs; write?' % num_changed_fields)
    if choice == 'yes':
        sheet.write_back()
        print('Wrote %d riffs' % num_changed_fields)

    return


def populate_instruments(
        rekordbox_state: library_organizer.RekordboxState,
        sheet: google_sheet.Sheet
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
            track.dirty_fields.add('Instruments')
            num_changed_fields += 1

    if num_changed_fields > 0:
        choice = get_user_choice('Write back %d Instruments fields?' % num_changed_fields)
        if choice == 'yes':
            sheet.write_back()
    return

def set_danceable(
        rekordbox_state: library_organizer.RekordboxState,
        sheet: google_sheet.Sheet
):
    num_changed_fields = 0
    for track in sheet.tracks:
        track.attributes['Danceable'] = track.attributes['Motivates Dancing'] or track.attributes['Sustains Dancing']
        if track.attributes['Danceable'] is not None:
            track.dirty_fields.add('Danceable')

    sheet.write_back()
    return


def merge_genres_and_flavors(
        rekordbox_state: library_organizer.RekordboxState,
        sheet: google_sheet.Sheet
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
        track.dirty_fields.add('Flavors')
        num_changed_fields += 1

    if num_changed_fields > 0:
        choice = get_user_choice('Write %d changed flavors?' % num_changed_fields)
        if choice == 'yes':
            sheet.write_back()
            print('Wrote %d changed flavors' % num_changed_fields)

    return

def fill_in_flavors(
        rekordbox_state: library_organizer.RekordboxState,
        sheet: google_sheet.Sheet
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
            track.dirty_fields.add('Flavors')
            num_changed_fields += 1

    choice = get_user_choice('Filled in %d flavors; write?' % num_changed_fields)
    if choice == 'yes':
        sheet.write_back()
        print('Wrote %d flavors' % num_changed_fields)

    return


def fill_in_genres(
        rekordbox_state: library_organizer.RekordboxState,
        sheet: google_sheet.Sheet
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
            track.dirty_fields.add('Genres')
            num_changed_fields += 1

    choice = get_user_choice('Filled in %d genres; write?' % num_changed_fields)
    if choice == 'yes':
        sheet.write_back()
        print('Wrote %d genres' % num_changed_fields)

    return



def fix_boolean_attributes(
        rekordbox_state: library_organizer.RekordboxState,
        sheet: google_sheet.Sheet
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
                last_set_row = track.row_num
            elif value is not None:
                boolean_attributes.remove(attribute)

    print('Detected Boolean attributes:')
    for attribute in boolean_attributes:
        print("    '%s'" % attribute)
    print('Detected last set row: %d' % last_set_row)

    reply = get_user_choice('Fix?')

    if reply == 'no':
        print('Aborting')
        return

    num_changed_fields = 0
    for track in sheet.tracks:
        if track.row_num > last_set_row:
            break

        for attribute in boolean_attributes:
            track.attributes[attribute] = 'T' if track.attributes[attribute] else 'F'
            track.dirty_fields.add(attribute)
            num_changed_fields += 1

    print('Writing back %d changed fields' % num_changed_fields)
    sheet.write_back()

    return




