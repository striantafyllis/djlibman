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

import time
import logging

import pytz

import djlib_config
from containers import *
from spotify_util import *

logger = logging.getLogger(__name__)

_singleton = None

class _SpotifyDiscography:
    def __init__(self):
        self._artists_by_id = None
        self._artists_by_name = None

        self._albums = Doc('spotify_albums')
        self._artist_albums_last_check = Doc('spotify_artist_albums_last_check')

        self._open_artist_docs = {
            'tracks': {},
            'albums': {}
        }

        return

    def _init_artists(self):
        if self._artists_by_id is not None:
            return

        start_time = time.time()

        listening_history = ListeningHistory()

        self._artists_by_id = get_track_artists(listening_history)

        self._artists_by_name = self._artists_by_id.set_index(keys='artist_name', inplace=False)

        # if there are names with multiple IDs, keep the last one
        self._artists_by_name = self._artists_by_name.loc[
            self._artists_by_name.index.duplicated(keep='last')
        ]

        return

    def get_spotify_artists(self):
        self._init_artists()
        return self._artists_by_id

    def get_spotify_artist_id(self, artist_name):
        self._init_artists()

        if artist_name in self._artists_by_name.index:
            return self._artists_by_name.loc[artist_name, 'artist_id']
        else:
            raise ValueError(f'Spotify artist with name {artist_name} not found.')

    def get_spotify_artist_name(self, artist_id):
        self._init_artists()

        if artist_id in self._artists_by_id.index:
            return self._artists_by_id.loc[artist_id, 'artist_name']
        else:
            raise ValueError(f'Spotify artist with id {artist_id} not found.')

    def _get_artist_doc(self, doc_type: str, artist_id: str, artist_name: str) -> Doc:
        assert doc_type in ['albums', 'tracks']

        if artist_id in self._open_artist_docs[doc_type]:
            return self._open_artist_docs[doc_type][artist_id]

        artist_doc_dir = os.path.join(djlib_config.default_dir, f'spotify-artist-{doc_type}')

        artist_doc_files = os.listdir(artist_doc_dir)

        artist_doc_file = None
        artist_doc_pattern = re.compile(r'^artist-%s-%s-.*\.csv$' % (doc_type, artist_id))
        for file in artist_doc_files:
            if re.match(artist_doc_pattern, file):
                artist_doc_file = file
                break

        if artist_doc_file is None:
            artist_doc_file = f'artist-{doc_type}-{artist_id}-{artist_name.replace('/', '-')}.csv'
            new_file = True
        else:
            new_file = False

        if doc_type == 'albums':
            index_column = 'album_id'
            datetime_columns = ['release_date']
        elif doc_type == 'tracks':
            index_column = 'spotify_id'
            datetime_columns = ['release_date', 'added_at']
        else:
            assert False

        artist_doc = Doc(
            name=f'artist {doc_type} {artist_id} {artist_name}',
            path=os.path.join(artist_doc_dir, artist_doc_file),
            type='csv',
            index_column=index_column,
            datetime_columns=datetime_columns,
            backups=0,
            create=new_file,
            overwrite=new_file,
            modify=True
        )

        self._open_artist_docs[doc_type][artist_id] = artist_doc

        return artist_doc

    def _refresh_artist_albums(self, artist_id, artist_name, refresh_days):
        if refresh_days is None or refresh_days < 0:
            raise ValueError(f'Invalid value for refresh_days: {refresh_days}')

        artist_albums_last_check = self._artist_albums_last_check.get_df()

        if artist_id not in artist_albums_last_check.index:
            logger.debug('Artist %s %s: albums have never been fetched', artist_id, artist_name)
            refresh = True

        else:
            last_check_time = artist_albums_last_check.loc[artist_id, 'check_time']

            now = pd.Timestamp.utcnow()
            past_days = (now - last_check_time).days

            refresh = past_days >= refresh_days

            logger.debug('Artist %s %s: albums were last fetched %s, %d days ago; %srefreshing',
                         artist_id, artist_name,
                         last_check_time, past_days,
                         ('' if refresh else 'not ')
                         )

        if refresh:
            print(f'Fetching albums for artist: {artist_id} {artist_name}...', end='')

            artist_albums = djlib_config.spotify.get_artist_albums(artist_id)

            new_artist_albums_idx = artist_albums.index.difference(self._albums.get_df().index, sort=False)
            new_artist_albums = artist_albums.loc[new_artist_albums_idx]

            print(f' {len(artist_albums)} fetched, {len(new_artist_albums)} new')

            self._albums.append(new_artist_albums, prompt=False)
            self._albums.write()

            self._artist_albums_last_check.get_df().loc[artist_id, 'check_time'] = pd.Timestamp.utcnow()
            self._artist_albums_last_check.write(force=True)

        return

    def _form_track_from_signature_group(self, same_sig_tracks):
        """Returns a dataframe with a single row that's the best representative of the
           entire track group.
        """

        # a good place to insert complicated breakpoints...
        # signature = same_sig_tracks.signature.iloc[0]

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
            # this usually happens in very pathological situations that don't interest us
            return desirable_tracks
            # desirable_tracks = same_sig_tracks

        # try to find an extended mix if possible
        extended_tracks = desirable_tracks.loc[
            desirable_tracks.apply(is_extended_edit, axis=1)
        ]
        if len(extended_tracks) > 0:
            desirable_tracks = extended_tracks

        # select the oldest ID
        if len(desirable_tracks) > 1:
            desirable_tracks.sort_values(by='release_date', inplace=True)
        track = desirable_tracks.iloc[:1]

        # Combine the popularities of the tracks. For now I just add them up,
        # and then also add the number of duplicates - because a track that gets
        # reposted in more albums is arguably more popular.
        popularity = same_sig_tracks.popularity.sum() + len(same_sig_tracks) - 1
        track['popularity'] = popularity

        assert isinstance(track, pd.DataFrame)
        assert len(track) == 1

        return track

    def _deduplicate_tracks(self, artist_tracks):
        artist_tracks['signature'] = artist_tracks.apply(
            get_track_signature,
            axis=1
        )

        # not necessary but helps debugging
        # artist_tracks.sort_values(by='name', inplace=True)

        gby = artist_tracks.groupby(by='signature', as_index=False, sort=False, group_keys=False)

        dedup_tracks = gby.apply(
            func=lambda group: self._form_track_from_signature_group(group)
        )

        return dedup_tracks

    def _artist_id_and_name(self, artist_id, artist_name):
        """Common operation on artist_id, artist_name arguments"""
        if artist_id is None:
            if artist_name is None:
                raise ValueError('At least one of artist_id or artist name must be specified.')
            artist_id = self.get_spotify_artist_id(artist_name)
        if artist_name is None:
            artist_name = self.get_spotify_artist_name(artist_id)

        return artist_id, artist_name


    def initialize_artist_albums(self,
                                 artist_id=None,
                                 artist_name=None):
        """One-time code to switch the database format"""

        artist_id, artist_name = self._artist_id_and_name(artist_id, artist_name)

        print(f'Building artist albums doc for {artist_id} {artist_name}')

        artist_tracks = self._get_artist_doc('tracks', artist_id, artist_name)

        if len(artist_tracks) == 0:
            print(f'    No tracks; nothing to do.')
            return

        artist_albums = self._get_artist_doc('albums', artist_id, artist_name)

        artist_album_ids = pd.Index(artist_tracks.get_df().album_id)
        artist_album_ids = artist_album_ids[~artist_album_ids.duplicated()]

        print(f'    {len(artist_tracks)} tracks, {len(artist_album_ids)} albums')

        spotify_albums = Doc('spotify_albums')

        artist_album_ids_in_doc = artist_album_ids.intersection(spotify_albums.get_df().index, sort=False)

        missing_artist_album_ids = artist_album_ids.difference(artist_album_ids_in_doc, sort=False)

        if len(missing_artist_album_ids) > 0:
            tracks_with_missing_artist_album_ids = artist_tracks.get_df().loc[
                artist_tracks.get_df().apply(
                    lambda track: track['album_id'] in missing_artist_album_ids,
                    axis=1
                )
            ]
            print(f'    *** {len(missing_artist_album_ids)} albums with '
                  f'{len(tracks_with_missing_artist_album_ids)} tracks missing from spotify_albums')
            artist_tracks.remove(tracks_with_missing_artist_album_ids)
            artist_tracks.write()

        artist_albums = spotify_albums.get_df().loc[artist_album_ids_in_doc]

        artist_albums_doc = self._get_artist_doc('albums', artist_id, artist_name)

        artist_albums_doc.set_df(artist_albums)
        artist_albums_doc.write()

        return

    def get_artist_discography(self, *,
                               artist_id=None,
                               artist_name=None,
                               refresh_days=30,
                               deduplicate_tracks=False,
                               cache_only=False):
        """
        Returns all known tracks for the artist. At least one of (artist_id, artist_name)
        must be specified.
           refresh_days (default 30): check for new tracks if the last check was more than
                                      this many days ago. If None, don't check.
           deduplicate_tracks (default False): try to filter out duplicate releases of the same track
                                               (potentially expensive)
        """

        artist_id, artist_name = self._artist_id_and_name(artist_id, artist_name)

        if not cache_only:
            self._refresh_artist_albums(artist_id, artist_name, refresh_days)

        artist_tracks = self._get_artist_doc('tracks', artist_id, artist_name).get_df()

        if len(artist_tracks) > 0 and deduplicate_tracks:
            artist_tracks = self._deduplicate_tracks(artist_tracks)

        return artist_tracks


def get_instance():
    global _singleton
    if _singleton is None:
        _singleton = _SpotifyDiscography()
    return _singleton

