#!/usr/bin/env python

import sys
import argparse
import library_organizer
import rekordbox
# import google_sheet
# import cli

def main():
    parser = argparse.ArgumentParser(
        prog='library_organizer.py',
        description='Organizes my music library between Rekordbox, Spotify and YouTube with help from some Google docs'
    )
    parser.add_argument('--playlist_dir', default=rekordbox.default_playlist_dir)
    parser.add_argument('--rekordbox_xml', default=rekordbox.default_rekordbox_xml)
    # parser.add_argument('--google_sheet_id', default=google_sheet.DEFAULT_SPREADSHEET_ID)
    # parser.add_argument('--google_sheet_page', default=google_sheet.DEFAULT_SPREADSHEET_PAGE)

    args = parser.parse_args()

    playlist_dir = args.playlist_dir

    rekordbox_state = library_organizer.read_rekordbox(args.rekordbox_xml)

    # sheet = google_sheet.parse_sheet(args.google_sheet_id, args.google_sheet_page)
    #
    # library_organizer.rekordbox_stats(rekordbox_state)
    # library_organizer.rekordbox_sanity_checks(rekordbox_state)
    #
    # library_organizer.sheet_stats(sheet)
    #
    # library_organizer.cross_reference_rekordbox_to_google_sheet(rekordbox_state, sheet)
    #
    # library_organizer.sheet_vs_rekordbox_sanity_checks(sheet, rekordbox_state)
    #
    # cli.cli_loop(
    #     rekordbox_state,
    #     sheet,
    #     playlist_dir
    # )

    return 0


if __name__ == '__main__':
    exit_value = main()
    sys.exit(exit_value)
