
import djlib_config
from containers import *

def format_track_for_search(track):
    """Creates a search string that's more likely to generate matches out of a
    track's artists and title."""

    if isinstance(track, str):
        string = track.lower()
    else:
        string = get_attrib_or_fail(track, ['Title', 'name'])
        string = string.lower()

    # Remove some things that are usually in Rekordbox but not in Spotify
    string = re.sub(r'(feat\.|featuring)', '', string, flags=re.IGNORECASE)
    string = re.sub(r'original( mix|$)', '', string, flags=re.IGNORECASE)
    string = re.sub(r'extended mix', '', string, flags=re.IGNORECASE)

    if not isinstance(track, str):
        if 'artist_names' in track:
            artists_list = [artist.lower() for artist in track['artist_names'].split('|')]

            # Work around a big difference between Spotify and everyone else:
            # In Spotify, remix artists are in the artists' list; in other services they aren't
            filtered_artists_list = [artist for artist in artists_list if artist not in string]

            artists = ' '.join(filtered_artists_list)
        elif 'Artists' in track:
            artists = track['Artists'].replace(',', ' ').lower()
        else:
            raise Exception("None of the attributes %s are present in series %s" % (
                ['artists', 'Artists'],
                track
            ))

        # sort the words in the artist string; this is necessary because Spotify and Rekordbox
        # often list artists in different order.
        # This will also mix up first and last names of the same artist; I don't see a way to avoid this
        artist_words = artists.split()
        artist_words.sort()

        string = ' '.join(artist_words) + ' ' + string

    # replace sequences of non-word characters with a single space
    # EXCEPT: Special-case the hyphen because some names contain it (e.g. Kay-D)
    # Then take care of hyphens that are not in names
    string = re.sub(r'[^-\w]+', ' ', string)
    string = re.sub(r'\B-\B', ' ', string)

    # replace multiple spaces with single space
    string = ' '.join(string.split())

    # get rid of capitalization problems
    string = string.lower()

    return string


def pretty_print_tracks(tracks, indent='', enum=False, ids=True, extra_attribs=[]):
    num_tracks = len(tracks)
    if num_tracks == 0:
        return

    if hasattr(tracks, 'get_df'):
        tracks = tracks.get_df()

    if isinstance(tracks, pd.DataFrame):
        if tracks.empty:
            return
        tracks = tracks.iloc

    for i in range(num_tracks):
        sys.stdout.write(indent)
        if enum:
            sys.stdout.write(f'{i+1}. ')

        sys.stdout.write(format_track(tracks[i], id=ids, extra_attribs=extra_attribs) + '\n')

    sys.stdout.flush()

    return

def artist_name_condition(name_condition, mode='or'):
    if mode not in ['and', 'or']:
        raise Exception("Invalid mode '%s'" % mode)

    return lambda row: list_condition(lambda el: name_condition(el['name']))(row.artists)


def artist_stats(tracks, count_cutoff=10):
    artists_to_counts = {}

    def _handle_artists(artists):
        for artist in artists:
            artists_to_counts[artist['name']] = artists_to_counts.get(artist['name'], 0) + 1

    tracks.artists.apply(_handle_artists)

    artist_names = list(artists_to_counts)
    artist_names.sort(key=lambda x: artists_to_counts[x], reverse=True)

    for artist_name in artist_names:
        count = artists_to_counts[artist_name]
        if count < count_cutoff:
            break
        print('%s: %d' % (artist_name, count))

    return


def pretty_print_albums(albums, indent='', enum=False):
    for i, album in enumerate(albums):
        sys.stdout.write(indent)
        if enum:
            sys.stdout.write(f'{i+1}: ')
        sys.stdout.write(f'{album['id']}: {album['name']} ({album['total_tracks']} tracks)\n')

    sys.stdout.flush()

    return

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
                artists.loc[id] = { 'artist_id': id, 'artist_name': name }

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

    # not_found_artists = {}

    for track in tracks.itertuples(index=False):
        if pd.isna(track.artist_ids):
            continue
        artist_ids = track.artist_ids.split('|')
        # artist_names = track.artist_names.split('|')

        for i, artist_id in enumerate(artist_ids):
            if artist_id not in artists.index:
                pass
                # not_found_artists[artist_id] = artist_names[i]
            else:
                artists.loc[artist_id, track_count_column] += 1

    # if len(not_found_artists) > 0:
    #     print(f'{len(not_found_artists)} artists not found in artist dataframe')

    return


class Queue(Doc):
    """A special doc for the Spotify queue; it sets added_at to now() when adding tracks."""
    def __init__(
            self,
            name: str = 'queue', *,
            modify=True,
            create=True,
            overwrite=True,
            **kwargs):
        super(Queue, self).__init__(
            name=name,
            type='csv',
            modify=modify,
            create=create,
            overwrite=overwrite,
            index_name='spotify_id',
            header=0,
            datetime_columns=['release_date', 'added_at'],
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

    def remove(self, other, prompt=None, force=False):
        if not force:
            raise ValueError('Why remove from listening history?')
        super(ListeningHistory, self).remove(other, prompt)

    def _ensure_track_signatures(self):
        self._ensure_df()

        self._track_signatures = pd.Index(
            self._df.apply(
                get_track_signature,
                axis=1
            )
        )

        return

    def filter(self, other: Container, prompt=None, silent=False):
        self._ensure_track_signatures()

        if not isinstance(other, Container):
            raise ValueError("'other' argument must be a container")

        other_df = other.get_df()

        if len(other_df) == 0:
            return

        if other_df.index.name != 'spotify_id':
            raise ValueError('Only Spotify tracks indexed by spotify_id can be filtered '
                             'through listening history')

        other_not_listened_index = other_df.index.difference(self._df.index, sort=False)
        filtered_through_spotify_id = len(other_df.index) - len(other_not_listened_index)

        if filtered_through_spotify_id != 0:
            other_df = other_df.loc[other_not_listened_index]

        # for some reason Pandas.apply doesn't work correctly if the dataframe is empty;
        # it returns an empty dataframe instead of a boolean array
        if len(other_df) > 0:
            other_not_listened_sigs = other_df.apply(
                lambda track: get_track_signature(track) not in self._track_signatures,
                axis=1
            )

            other_df_not_listened_sigs = other_df.loc[other_not_listened_sigs]

            filtered_through_track_sigs = len(other_df) - len(other_df_not_listened_sigs)

            if filtered_through_track_sigs != 0:
                other_df = other_df_not_listened_sigs
        else:
            filtered_through_track_sigs = 0

        filtered = filtered_through_spotify_id + filtered_through_track_sigs

        if filtered != 0:
            if not silent:
                filtered_tracks = other._df.loc[
                    other._df.index.difference(other_df.index, sort=False)
                ]

                print(f'{other.get_name()}: removing {filtered} tracks from listening '
                      f'history - {filtered_through_spotify_id} by spotify ID and '
                      f'{filtered_through_track_sigs} by track signatures')

                pretty_print_tracks(filtered_tracks)

            if other._should_prompt(prompt):
                choice = get_user_choice('Proceed?')
                if choice != 'yes':
                    return

            # not calling set_df() here because we don't want the overwrite
            # check and the ID reconciliation
            other._df = other_df
            other._changed = True

        elif not silent:
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

