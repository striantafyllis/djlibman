"""
Functions to maintain artist discographies.
These need special treatment because they generate too many Spotify REST requests,
which are slow, and may also lead Spotify to rate-limit me.

In the Spotify API, getting an artist's discography has to be done in two steps:
- First, get all the albums that contain the artist's tracks
- Then, get all the tracks for each album, and filter out the artist's tracks

Both these stages can be cached to disk, so that they don't have to be repeated every time.
- Album tracks should never change; once recovered, they can be used forever.
- An artist's albums can change, but there is no need to refresh them very frequently;
  once a week or so should be enough.
"""

import os
import os.path
import time
import re

import djlib_config
from containers import *
from djlib_config import discography_cache_dir

def _verbose():
    return djlib_config.discography_verbose

_SECONDS_PER_DAY = 24 * 3600


def get_artists(tracks: Union[Container, pd.DataFrame]):
    """Returns a dictionary - id to artist name - from the argument - which
       should be a DF of Spotify tracks or similar."""

    if isinstance(tracks, Container):
        tracks = tracks.get_df()

    artist_id_to_name = {}

    for i in range(len(tracks)):
        artist_ids = tracks.artist_ids.iat[i].split('|')
        artist_names = tracks.artist_names.iat[i].split('|')

        if not isinstance(artist_ids, list) and pd.isna(artist_ids):
            continue

        for j in range(len(artist_ids)):
            id = artist_ids[j]
            name = artist_names[j]

            existing_name = artist_id_to_name.get(id)
            if existing_name is None:
                artist_id_to_name[id] = name
            elif existing_name != name:
                # Sometimes this happens if an artist changes their Spotify name.
                # In that case, keep the latest name.
                # print(f'Warning: Artist ID {id} associated with two different names: '
                #       f'{existing_name} and {name}')
                artist_id_to_name[id] = name

    return artist_id_to_name


def find_spotify_artist(artist_name):
    """Returns the Spotify entry for the artist with the given name as a dictionary.
    The ID is in there also.
    The search is first done in the library, then expands to all of Spotify if necessary."""

    listening_history = Doc('listening_history')

    artists = get_artists(listening_history)

    candidate_ids = []

    for id, name in artists.items():
        if name == artist_name:
            candidate_ids.append(id)

    if len(candidate_ids) > 1:
        raise ValueError(f'Multiple IDs found for artist {artist_name}: {candidate_ids}')
    elif len(candidate_ids) == 0:
        raise ValueError(f'Artist {artist_name} not found')

    return candidate_ids[0]

def _get_from_cache(name, cache_file_name, ttl_days=None):
    cache_file_path = os.path.join(djlib_config.discography_cache_dir, cache_file_name)

    cache_file_doc = Doc(name,
                         create=True,
                         path=cache_file_path,
                         type='csv',
                         datetime_columns=['release_date', 'added_at'])

    # try to find the artist albums in the cache
    if cache_file_doc.exists():
        if _verbose() >= 4:
            print(f'{name} found in cache: {cache_file_path}')

        if ttl_days is not None:
            file_creation_time = cache_file_doc.getmtime()

            file_age = time.time() - file_creation_time

            if file_age > ttl_days * _SECONDS_PER_DAY:
                if _verbose() >= 4:
                    print(f'{cache_file_path} is too old; file age {file_age:.0f}, '
                          f'TTL {djlib_config.artist_albums_ttl_days}')

                cache_file_doc.delete()
    else:
        if _verbose() >= 4:
            print(f'{name} not found in cache')

    return cache_file_doc


def _get_artist_albums(artist_id, artist_name):
    cache_file_doc = _get_from_cache(f'artist albums for {artist_name}',
                                     f'artist-albums-{artist_id}-{artist_name.replace('/', '-')}.csv',
                                     ttl_days=djlib_config.artist_albums_ttl_days)

    if cache_file_doc.exists():
        artist_albums = cache_file_doc.get_df()
    else:
        artist_albums = djlib_config.spotify.get_artist_albums(artist_id)

    if _verbose() >= 2:
        total_tracks = artist_albums.total_tracks.sum()
        print(f'Artist {artist_name}: found {len(artist_albums)} albums with {total_tracks} total tracks')

    if not cache_file_doc.exists() and djlib_config.artist_albums_ttl_days > 0:
        if _verbose() >= 4:
            print(f'Artist albums for {artist_name}: writing cache file {cache_file_doc._doc._path}')

        cache_file_doc.append(artist_albums, prompt=False)
        cache_file_doc.write()

    return artist_albums

def _get_album_tracks(album_id, album_name):
    cache_file_doc = _get_from_cache(
        f'album tracks for {album_name}',
        f'album-tracks-{album_id}-{album_name.replace('/', '-')}.csv')

    if cache_file_doc.exists():
        album_tracks = cache_file_doc.get_df()
    else:
        album_tracks = djlib_config.spotify.get_album_tracks(album_id)

    if _verbose() >= 3:
        print(f'Album {album_name}: found {len(album_tracks)} tracks')

    if not cache_file_doc.exists():
        if _verbose() >= 4:
            print(f'Album {album_name}: writing cache file {cache_file_doc._doc._path}')

        cache_file_doc.append(album_tracks, prompt=False)
        cache_file_doc.write()

    return album_tracks

def _filter_tracks_by_artist(artist_id, tracks):
    return tracks.loc[
        tracks.apply(
            lambda track: artist_id in track['artist_ids'].split('|'),
            axis=1
        )
    ]

def _get_track_signature(track):
    """Returns a string that should uniquely identify the track in most contexts;
       the string contains the artist names and the track title separated by \u2013"""
    name = track['name']
    artist_ids = track['artist_ids'].split('|')

    # get rid of parenthesized combinations of uppercase letters and numbers - these are usually label codes
    name = re.sub(r'(\[|\()[A-Z]{3,100} ?[0-9]+(\]|\))', '', name)

    name = name.upper()

    # get read of "featuring ...", "feat. " etc.
    name = re.sub(r'FEAT(\.|URING) .*', '', name)

    for s in ['(', ')', '[', ']', '-', ' AND ', ' X ', 'EXTENDED', 'ORIGINAL', 'REMIX', 'MIXED', 'MIX', 'RADIO', 'EDIT']:
        name = name.replace(s, '')

    # get rid of whitespace differences
    name = ' '.join(name.split())

    artist_ids.sort()

    return tuple(artist_ids + [name])

def _form_track_from_signature_group(same_sig_tracks, listening_history):
    """Returns a dataframe with a single row that's the best representative of the
       entire track group.
    """
    # a good place to insert complicated breakpoints...
    # signature = same_sig_tracks.signature[0]
    # if 'A LONELY PINK CLOUD' in signature:
    #     pass

    if len(same_sig_tracks) == 1:
        track = same_sig_tracks
    else:
        # if possible, select the track ID that is in listening history; this way it will get filtered.
        ids_in_lh = same_sig_tracks.index.intersection(listening_history.get_df().index, sort=False)

        if len(ids_in_lh) >= 1:
            track = same_sig_tracks.loc[[ids_in_lh[0]]]
        else:
            # remove undesirable edits
            def is_desirable_edit(track):
                name = track['name'].upper()
                is_undesirable = (
                        name.endswith(' - MIXED') or
                        name.endswith('(MIXED)') or
                        name.endswith('[MIXED]') or
                        'RADIO EDIT' in name
                )
                return not is_undesirable

            def is_extended_edit(track):
                name = track['name'].upper()
                is_extended = 'EXTENDED' in name or ' X ' in name
                return is_extended

            desirable_tracks = same_sig_tracks.loc[
                same_sig_tracks.apply(is_desirable_edit, axis=1)
            ]

            if len(desirable_tracks) == 0:
                # This happens very rarely - e.g. all versions of a track on Spotify are radio edits -
                # but it does happen. Often Beatport has a normal version, so we don't want to exclude these.
                desirable_tracks = same_sig_tracks

            # try to find an extended mix if possible
            extended_tracks = desirable_tracks.loc[
                desirable_tracks.apply(is_extended_edit, axis=1)
            ]
            if len(extended_tracks) > 0:
                desirable_tracks = extended_tracks

            # select the oldest ID
            desirable_tracks.sort_values(by='release_date', inplace=True)
            track = same_sig_tracks.iloc[:1]

        # Combine the popularities of the tracks. For now I just add them up,
        # and then also add the number of duplicates - because a track that gets
        # reposted in more albums is arguably more popular.
        popularity = same_sig_tracks.popularity.sum() + len(same_sig_tracks) - 1
        track['popularity'] = popularity

    assert isinstance(track, pd.DataFrame)
    assert len(track) == 1

    return track


def get_artist_discography(artist_name):
    artist_id = find_spotify_artist(artist_name)

    artist_albums = _get_artist_albums(artist_id, artist_name)

    artist_tracks = pd.concat(
        artist_albums.apply(
            lambda album: _filter_tracks_by_artist(
                artist_id,
                _get_album_tracks(album['album_id'], album['name'])),
            axis=1
        )
        .values
    )

    assert len(artist_tracks) >= len(artist_albums)

    # for debugging - remove
    # artist_tracks = artist_tracks.loc[
    #     artist_tracks.apply(lambda t: 'Getting Closer' in t['name'], axis=1)
    # ]

    if _verbose() >= 2:
        print(f'{artist_name}: found {len(artist_tracks)} tracks')

    artist_tracks['signature'] = artist_tracks.apply(
        _get_track_signature,
        axis=1
    )

    artist_tracks.sort_values(by='name', inplace=True)

    gby = artist_tracks.groupby(by='signature', as_index=False, sort=False, group_keys=False)

    listening_history = Doc('listening_history')

    dedup_tracks = gby.apply(
        func=lambda group: _form_track_from_signature_group(group, listening_history)
    )

    assert len(dedup_tracks) == len(gby)

    # not necessary but makes debugging easier
    # dedup_tracks.sort_values(by='name', inplace=True)

    if _verbose() >= 1:
        print(f'{artist_name}: {len(dedup_tracks)} tracks left after deduplication')

    dedup_tracks.drop(labels='signature', axis=1, inplace=True)

    return dedup_tracks
