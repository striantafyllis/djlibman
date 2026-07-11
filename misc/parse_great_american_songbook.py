
import os

import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO

from spyroslib.containers import Container, Wrapper, Doc

def parse():
    file = os.path.join(os.environ['HOME'], 'Music', 'djlib',
                        'Great American Songbook - Wikipedia.html')

    csv_file = os.path.join(os.environ['HOME'], 'Music', 'djlib',
                        'Great American Songbook.csv')

    fh = open(file, "r", encoding="utf-8")
    soup = BeautifulSoup(fh, 'html.parser')
    fh.close()

    tables = soup.find_all('table')

    song_table = tables[2]

    dfs = pd.read_html(StringIO(str(song_table)))

    df = dfs[0]

    # remove empty rows
    df = df.loc[~df['Song title'].isna()]

    # remove the useless Notes column
    df = df.drop(columns=['Notes'])

    df.sort_values(by=['Year'], inplace=True)

    df['Year'] = df['Year'].astype(int)

    df['Song title'] = df['Song title'].str.replace('"', '').replace("\\", "")

    wrapper = Wrapper(contents=df, name='Great American Songbook')

    wrapper.to_csv(csv_file)

    # read it back to make sure
    csv = Doc(name='Great American Songbook', path=csv_file)
    df2 = csv.get_df()

    return

def main():
    parse()
    return

if __name__ == '__main__':
    main()


