
import djlib_config
from containers import *

def get_track_signature(track):
    """Returns a value that should uniquely identify the track in most contexts;
       the value is a tuple contains the artist names and the track title"""
    name = track['name']
    artist_names = track['artist_names'].upper().split('|')

    # get rid of parenthesized combinations of uppercase letters and numbers - these are usually label codes
    name = re.sub(r'(\[|\()[A-Z]{3,100} ?[0-9]+(\]|\))', '', name)

    name = name.upper()

    # get read of "featuring ...", "feat. " etc.
    name = re.sub(r'FEAT(\.|URING) .*', '', name)

    for s in ['(', ')', '[', ']', '-', ' AND ', ' X ', 'EXTENDED', 'ORIGINAL', 'REMIX', 'MIXED', 'MIX', 'RADIO', 'EDIT']:
        name = name.replace(s, '')

    # get rid of whitespace differences
    name = ' '.join(name.split())

    artist_names.sort()

    return tuple(artist_names + [name])


def text_to_spotify_track(text):
    # Remove everything until the first letter character - e.g. numbers like 1.
    text1 = re.sub(r'^[0-9\W]+', '', text)

    # Remove everything in square or curly brackets; these are used for things like label name, [Free DL] etc.
    text2 = re.sub(r'(\[|\{)[^]}]*(\]|\})', '', text1)

    search_string = format_track_for_search(text2)

    print(f'Searching for track: {text} with search string: {search_string}')
    spotify_tracks = djlib_config.spotify.search(search_string)

    if len(spotify_tracks) == 0:
        print('    No search results from Spotify!')
    else:
        spotify_sequences = spotify_tracks.apply(format_track_for_search, axis=1)
        result = fuzzy_one_to_one_mapping([search_string], spotify_sequences,
                                          cutoff_ratio=0.0,
                                          # this is necessary because Spotify places artist names differently
                                          tokenwise=True,
                                          asciify=True)

        assert len(result['pairs']) <= 1

        if len(result['pairs']) == 1:
            mapping = result['pairs'][0]
            assert mapping['index1'] == 0
            spotify_idx = mapping['index2']

            spotify_track = spotify_tracks.iloc[spotify_idx]

            print(f'    Best Spotify match: {format_track(spotify_track)}')
            print(f'    Match ratio: {mapping['ratio']:.2f}')
            if mapping['ratio'] >= djlib_config.fuzzy_match_automatic_accept_threshold:
                print('    Accepted automatically')
                return spotify_track
            else:
                choice = get_user_choice('Accept?')

                if choice == 'yes':
                    return spotify_track

    print('    Automatic match failed; going through search results in sequence.')

    for i in range(len(spotify_tracks)):
        spotify_track = spotify_tracks.iloc[i]
        print(f'Option {i + 1}: {format_track(spotify_track)}')
        choice = get_user_choice('Accept?', options=['yes', 'next', 'give up'])
        if choice == 'yes':
            return spotify_track
        elif choice == 'next':
            continue
        elif choice == 'give up':
            break

    print(f'    No Spotify track found for text {text}')
    return None


def get_track_artists(tracks: Union[Container, pd.DataFrame]):
    """Forms a dataframe artist_id: artist from a tracks dataframe"""
    if isinstance(tracks, Container):
        tracks = tracks.get_df()

    if 'artist_ids' not in tracks.columns:
        raise ValueError('No artist_ids column in tracks')
    if 'artist_names' not in tracks.columns:
        raise ValueError('No artist_names column in tracks')

    artists = pd.DataFrame(columns=['artist_id', 'artist_name'],
                           index=pd.Index([], name='artist_id'))

    for i in range(len(tracks)):
        artist_ids = tracks.artist_ids.iat[i].split('|')
        artist_names = tracks.artist_names.iat[i].split('|')

        if not isinstance(artist_ids, list) and pd.isna(artist_ids):
            continue

        for j in range(len(artist_ids)):
            id = artist_ids[j]
            name = artist_names[j]

            if id in artists.index:
                if artists.loc[id, 'artist_name'] != name:
                    # Sometimes this happens if an artist changes their Spotify name.
                    # In that case, keep the latest name.
                    # print(f'Warning: Artist ID {id} associated with two different names: '
                    #       f'{existing_name} and {name}')
                    artists.loc[id, 'artist_name'] = name
            else:
                artists.loc[id] = {'artist_id': id, 'artist_name' : name, 'track_count' : 1}

    artists = artists.convert_dtypes()

    return artists

def add_artist_track_counts(artists: pd.DataFrame, tracks: pd.DataFrame, track_count_column: str):
    """Adds track counts for each artist.
    """

    if artists.index.name != 'artist_id':
        raise ValueError(f'Artist dataframe is not indexed by artist_id')
    if 'artist_ids' not in tracks.columns or 'artist_names' not in tracks.columns:
        raise ValueError(f'Track dataframe does not contain artist_ids')

    if track_count_column not in artists.columns:
        artists[track_count_column] = 0

    not_found_artists = {}

    for track in tracks.itertuples(index=False):
        if pd.isna(track.artist_ids):
            continue
        artist_ids = track.artist_ids.split('|')
        artist_names = track.artist_names.split('|')

        for i, artist_id in enumerate(artist_ids):
            if artist_id not in artists.index:
                not_found_artists[artist_id] = artist_names[i]
            else:
                artists.loc[artist_id, track_count_column] += 1

    if len(not_found_artists) > 0:
        print(f'{len(not_found_artists)} artists not found in artist dataframe')

    return


class Queue(Doc):
    """A special doc for the Spotify queue; it sets added_at to now() when adding tracks."""
    def __init__(
            self,
            name: str = 'queue', *,
            modify=True,
            create=True,
            overwrite=True,
            index_name='spotify_id',
            **kwargs):
        super(Queue, self).__init__(
            name=name,
            modify=modify,
            create=create,
            overwrite=overwrite,
            index_name=index_name,
            **kwargs
        )

    def _preprocess_before_append(self, df: pd.DataFrame):
        df = df.assign(added_at=pd.Timestamp.utcnow())
        return df


class ListeningHistory(Doc):
    """A special doc for the listening history. It supports filtering by signature along with ID,
       plus it prohibits some operations that don't make sense."""

    def __init__(
            self,
            name: str = 'listening_history'):
        super(ListeningHistory, self).__init__(
            name=name,
            modify=True,
            create=False,
            overwrite=False,
            index_name='spotify_id'
        )

        self._track_signatures = None

    def _rvalue_check(self, operation):
        raise ValueError(
            'ListeningHistory should not be added to or removed from other containers')

    def _preprocess_before_append(self, df: pd.DataFrame):
        df = df.assign(added_at=pd.Timestamp.utcnow())
        return df

    def append(self, other, prompt=None):
        super(ListeningHistory, self).append(other, prompt)

        self._track_signatures = None
        return

    def remove(self, other, prompt=None):
        raise ValueError('Why remove from listening history?')

    def _ensure_track_signatures(self):
        self._ensure_df()

        self._track_signatures = pd.Index(
            get_track_signature,
            axis=1
        )
        return

    def filter(self, other: Container, prompt=None):
        self._ensure_track_signatures()

        if not isinstance(other, Container):
            raise ValueError("'other' argument must be a container")

        other_df = other.get_df()

        if other_df.index.name != 'spotify_id':
            raise ValueError('Only Spotify tracks indexed by spotify_id can be filtered '
                             'through listening history')

        other_not_listened_index = other_df.index.difference(self._df.index, sort=False)
        filtered_through_spotify_id = len(other_df.index) - len(other_not_listened_index)

        if filtered_through_spotify_id != 0:
            other_df = other_df.loc[other_not_listened_index]

        other_not_listened_sigs = other_df.apply(
            lambda track: get_track_signature(track) not in self._track_signatures,
            axis=1
        )

        other_df_not_listened_sigs = other_df.loc[other_not_listened_sigs]

        filtered_through_track_sigs = len(other_df_not_listened_sigs) - len(other_df)

        if filtered_through_track_sigs != 0:
            other_df = other_df_not_listened_sigs

        filtered = filtered_through_spotify_id + filtered_through_track_sigs

        if filtered != 0:
            print(f'{other.get_name()}: removing {filtered} tracks from listening '
                  f'history - {filtered_through_spotify_id} by spotify ID and '
                  f'{filtered_through_track_sigs} by track signatures')

            if other._should_prompt(prompt):
                choice = get_user_choice('Proceed?')
                if choice != 'yes':
                    return

                # not calling set_df() here because we don't want the overwrite
                # check and the ID reconciliation
                other._df = other_df
                other._changed = True
        else:
            print(f'{other.get_name()}: no tracks in listening history')

        return


def find_spotify_artist(artist_name):
    """Returns the Spotify entry for the artist with the given name as a dictionary.
    The ID is in there also.
    The search is first done in the library, then expands to all of Spotify if necessary."""

    listening_history = ListeningHistory()

    artists = get_track_artists(listening_history)

    candidate_ids = []

    for id, name in artists.items():
        if name == artist_name:
            candidate_ids.append(id)

    if len(candidate_ids) > 1:
        raise ValueError(f'Multiple IDs found for artist {artist_name}: {candidate_ids}')
    elif len(candidate_ids) == 0:
        raise ValueError(f'Artist {artist_name} not found')

    return candidate_ids[0]

