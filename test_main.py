
import sys

import pandas as pd

from djlibman import *


def test_google_sheets_code():
    djlib = Doc('djlib')

    df = djlib.get_df()

    df.iloc[-4:, 7] = True
    df.iloc[-4:, 8] = False

    djlib.write(force=True)

    return

def main():
    like_tracks_from_text_file('data/tmp.txt')

if __name__ == '__main__':
    main()
    # test_google_sheets_code()
    # queue_maintenance_salsa(last_track='Llegamos')
    sys.exit(0)
