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

        self._open_artist_tracks_docs = {}

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

    def _get_artist_tracks_doc(self, artist_id: str, artist_name: str):
        if artist_id in self._open_artist_tracks_docs:
            return self._open_artist_tracks_docs[artist_id]

        artist_tracks_dir = os.path.join(djlib_config.default_dir, 'spotify-artist-tracks')

        artist_tracks_files = os.listdir(artist_tracks_dir)

        artist_tracks_file = None
        artist_tracks_pattern = re.compile(r'^artist-tracks-%s-.*\.csv$' % artist_id)
        for file in artist_tracks_files:
            if re.match(artist_tracks_pattern, file):
                artist_tracks_file = file
                break

        if artist_tracks_file is None:
            artist_tracks_file = f'artist-tracks-{artist_id}-{artist_name.replace('/', '-')}.csv'
            new_file = True
        else:
            new_file = False

        artist_tracks_doc = Doc(
            name=f'artist tracks {artist_id} {artist_name}',
            path=os.path.join(artist_tracks_dir, artist_tracks_file),
            type='csv',
            index_column='spotify_id',
            datetime_columns=['release_date', 'added_at'],
            backups=0,
            create=new_file,
            overwrite=new_file,
            modify=True
        )

        self._open_artist_tracks_docs[artist_id] = artist_tracks_doc

        return artist_tracks_doc

    def _refresh_artist_albums(self, artist_id, artist_name, refresh_days):
        if refresh_days is None or refresh_days < 0:
            return

        artist_albums_last_check = self._artist_albums_last_check.get_df()

        if artist_id not in artist_albums_last_check.index:
            logger.debug('Artist %s %s: albums have never been fetched', artist_id, artist_name)
            refresh = True
        else:
            last_check_time = artist_albums_last_check.loc[artist_id, 'check_time']

            now = pd.Timestamp.now()
            past_days = (now - last_check_time).days

            refresh = past_days >= refresh_days

            logger.debug('Artist %s %s: albums were last fetched %s, %d days ago; %srefreshing',
                         artist_id, artist_name,
                         last_check_time, past_days,
                         ('' if refresh else 'not ')
                         )

        if refresh:
            raise NotImplementedError('Refreshing artist albums')

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

    def get_artist_discography(self, *,
                               artist_name=None,
                               artist_id=None,
                               refresh_days=30,
                               deduplicate_tracks=False):
        """
        Returns all known tracks for the artist. At least one of (artist_id, artist_name)
        must be specified.
           refresh_days (default 30): check for new tracks if the last check was more than
                                      this many days ago. If None, don't check.
           deduplicate_tracks (default False): try to filter out duplicate releases of the same track
                                               (potentially expensive)
        """

        if artist_id is None:
            if artist_name is None:
                raise ValueError('At least one of artist_id or artist name must be specified.')
            artist_id = self.get_spotify_artist_id(artist_name)
        if artist_name is None:
            artist_name = self.get_spotify_artist_name(artist_id)

        self._refresh_artist_albums(artist_id, artist_name, refresh_days)

        artist_tracks = self._get_artist_tracks_doc(artist_id, artist_name).get_df()

        if len(artist_tracks) > 0 and deduplicate_tracks:
            artist_tracks = self._deduplicate_tracks(artist_tracks)

        return artist_tracks


def get_instance():
    global _singleton
    if _singleton is None:
        _singleton = _SpotifyDiscography()
    return _singleton

