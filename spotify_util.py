
import djlib_config
from containers import *


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


def text_file_to_spotify_tracks(text_file, target_playlist_name='L1 queue'):
    if target_playlist_name is None:
        target_playlist = None
    else:
        target_playlist = SpotifyPlaylist(target_playlist_name)

    lines = read_lines_from_file(text_file)

    print(f'Looking for {len(lines)} lines of text in Spotify' +
          (f'; adding to playlist {target_playlist_name}'
           if target_playlist_name is not None else ''))

    unmatched_lines = []
    for line in lines:
        spotify_track = text_to_spotify_track(line)

        if spotify_track is None:
            unmatched_lines.append(line)
        elif target_playlist is not None:
            # this avoids a Pandas warning
            spotify_track['added_at'] = pd.Timestamp.now()
            target_playlist.get_df().loc[spotify_track['spotify_id']] = spotify_track


    target_playlist.write(force=True)

    if len(unmatched_lines) == 0:
        print(f'Matched all {len(lines)} lines of text in Spotify')
    else:
        print(f'{len(unmatched_lines)} out of {len(lines)} were left unmatched:')
        for unmatched_line in unmatched_lines:
            print('    ' + unmatched_line)

    return


def get_track_artists(tracks: Union[Container, pd.DataFrame]):
    """Forms a dataframe artist_id: artist from a tracks dataframe"""
    if isinstance(tracks, Container):
        tracks = tracks.get_df()

    if 'artist_ids' not in tracks.columns:
        raise ValueError('No artist_ids column in tracks')
    if 'artist_names' not in tracks.columns:
        raise ValueError('No artist_names column in tracks')

    artists = pd.DataFrame(columns=['artist_id', 'artist_name', 'track_count'],
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
                artists.loc[id, 'track_count'] += 1
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

