
import sys

import pandas as pd

from djlibman import *


def main():
    # get_progressive_a_producers()

    # refresh_prog_a_producers()

    # discog_report_for_prog_a_producers()

    populate_queue()

    # debug_discography()

    # playlists_maintenance(do_spotify=False)

    # promote_set_tracks_to_a()

    # review_maintenance('DJ Progressive A Review',
    #                    ref_playlist='DJ Progressive A Review Ref',
    #                    method='liked+ref',
    #                    last_track='Disposition')

    return


def test_google_sheets_code():
    djlib = Doc('djlib')

    df = djlib.get_df()

    df.iloc[-4:, 7] = True
    df.iloc[-4:, 8] = False

    djlib.write(force=True)

    return


if __name__ == '__main__':
    main()
    # test_google_sheets_code()
    # queue_maintenance_salsa(last_track='Llegamos')
    sys.exit(0)
