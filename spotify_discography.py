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

        self._open_docs = {
            'artist-tracks': {},
            'artist-albums': {},
            'album-tracks': {},
        }

        return

    def _init_artists(self):
        if self._artists_by_id is not None:
            return

        listening_history = ListeningHistory()

        self._artists_by_id = get_track_artists(listening_history)

        self._artists_by_name = self._artists_by_id.set_index(keys='artist_name', inplace=False)

        # if there are names with multiple IDs, keep the last one
        self._artists_by_name = self._artists_by_name.loc[
            ~self._artists_by_name.index.duplicated(keep='last')
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

    def _get_doc(self, doc_type: str, id: str, name: str, force=False) -> Doc:
        if doc_type not in self._open_docs:
            raise ValueError(f'Unknown doc type {doc_type}')

        if id in self._open_docs[doc_type]:
            return self._open_docs[doc_type][id]

        doc_dir = os.path.join(djlib_config.default_dir, 'spotify-' + doc_type)

        doc_files = os.listdir(doc_dir)

        doc_file = None
        doc_pattern = re.compile(r'^%s-%s-.*\.csv$' % (doc_type, id))
        for file in doc_files:
            if re.match(doc_pattern, file):
                doc_file = file
                break

        if doc_file is None:
            doc_file = f'{doc_type}-{id}-{name.replace('/', '-')}.csv'
            new_file = True
        else:
            new_file = False

        if doc_type.endswith('albums'):
            index_column = 'album_id'
            datetime_columns = ['release_date']
        elif doc_type.endswith('tracks'):
            index_column = 'spotify_id'
            datetime_columns = ['release_date', 'added_at']
        else:
            assert False

        doc = Doc(
            name=f'{doc_type} {id} {name}',
            path=os.path.join(doc_dir, doc_file),
            type='csv',
            index_column=index_column,
            datetime_columns=datetime_columns,
            backups=0,
            create=new_file,
            overwrite=new_file or force,
            modify=True
        )

        self._open_docs[doc_type][id] = doc

        return doc

    def _refresh_artist_albums(self, artist_id, artist_name, *,
                               refresh_days=30, force=False):
        if refresh_days is None or refresh_days < 0:
            raise ValueError(f'Invalid value for refresh_days: {refresh_days}')

        artist_albums_doc = self._get_doc('artist-albums', artist_id, artist_name, force=force)

        if force:
            refresh = True
        elif not artist_albums_doc.exists():
            logger.debug('Artist %s %s: albums have never been fetched', artist_id, artist_name)
            refresh = True
        else:
            last_update = artist_albums_doc.getmtime()
            now = time.time()

            past_days = (now - last_update) / 24 / 3600

            refresh = past_days >= refresh_days

            logger.debug('Artist %s %s: albums were last updated at %s, %.1f days ago; '
                         '%srefreshing',
                         artist_id, artist_name,
                         pd.Timestamp(last_update), past_days,
                         ('' if refresh else 'not ')
                         )

        if not refresh:
            return

        print(f'Fetching albums for artist: {artist_id} {artist_name}...', end='')

        artist_albums = djlib_config.spotify.get_artist_albums(artist_id)

        if force:
            print(f' {len(artist_albums)} fetched; replacing {len(artist_albums_doc)} existing albums')

            artist_albums_doc.set_df(artist_albums)
        else:
            new_artist_albums_idx = artist_albums.index.difference(
                artist_albums_doc.get_df().index, sort=False)
            new_artist_albums = artist_albums.loc[new_artist_albums_idx]

            not_found_artist_albums_idx = artist_albums_doc.get_df().index.difference(
                artist_albums.index, sort=False)

            if len(not_found_artist_albums_idx) > 0:
                raise Exception(
                    f'Artist {artist_id} {artist_name}: {len(not_found_artist_albums_idx)} '
                    f'out of {len(artist_albums_doc)} existing albums were not found in the '
                    f'latest update; use force=True to replace the existing artist albums file')

            print(f' {len(artist_albums)} fetched, {len(new_artist_albums)} new, '
                  f'{len(artist_albums_doc) - len(new_artist_albums)} already there')

            artist_albums_doc.append(new_artist_albums, prompt=False)

        # write it even if we found no new albums, so that the file's mtime moves to now
        artist_albums_doc.write(force=True)

        return


    def _refresh_artist_tracks(self, artist_id, artist_name, *, force=False):
        artist_tracks_doc = self._get_doc('artist-tracks', artist_id, artist_name, force=force)
        artist_albums_doc = self._get_doc('artist-albums', artist_id, artist_name)

        orig_num_tracks = len(artist_tracks_doc)

        if not artist_albums_doc.exists():
            logger.debug('Artist %s %s: albums have never been fetched, nothing to do',
                         artist_id, artist_name)
            return

        if force:
            refresh = True
        elif not artist_tracks_doc.exists():
            logger.debug('Artist %s %s: tracks have never been written', artist_id, artist_name)
            refresh = True
        else:
            last_update = artist_tracks_doc.getmtime()

            artist_albums_last_update = artist_albums_doc.getmtime()

            refresh = last_update < artist_albums_last_update

            logger.debug('Artist %s %s: albums were last updated at %s, tracks were last updated '
                         'at %s; %srefreshing',
                         artist_id, artist_name,
                         pd.Timestamp(artist_albums_last_update), pd.Timestamp(last_update),
                         ('' if refresh else 'not ')
                         )

        if not refresh:
            return

        print(f'Writing tracks for artist: {artist_id} {artist_name}...', end='')

        if force:
            print(f" replacing {len(artist_tracks_doc)} existing tracks with tracks from "
                  f"{len(artist_albums_doc)} albums")

            artist_tracks_doc.truncate(prompt=False, silent=True)
            new_artist_albums = artist_albums_doc.get_df()
        else:
            albums_in_artist_tracks = pd.Index(artist_tracks_doc.get_df().album_id).unique()

            not_found_artist_albums = albums_in_artist_tracks.difference(
                artist_albums_doc.get_df().index, sort=False)

            if len(not_found_artist_albums) > 0:
                raise Exception(
                    f'Artist {artist_id} {artist_name}: artist tracks contain {len(not_found_artist_albums)} '
                    f'albums that are not in artist albums; use force=True to replace the existing '
                    f'artist tracks file'
                )

            new_artist_albums_idx = artist_albums_doc.get_df().index.difference(
                albums_in_artist_tracks, sort=False)

            new_artist_albums = artist_albums_doc.get_df().loc[new_artist_albums_idx]

            print(f" {len(albums_in_artist_tracks)} albums' tracks already there, getting "
                  f" {len(new_artist_albums)} new albums' tracks")

        for album in new_artist_albums.itertuples():
            album_tracks = self._get_album_tracks(album.album_id, album.name)

            artist_album_tracks = album_tracks.get_df().loc[
                album_tracks.get_df().apply(
                    lambda track: artist_id in track['artist_ids'].split('|'),
                    axis=1
                )
            ]

            artist_tracks_doc.append(artist_album_tracks, prompt=False, silent=True)


        print(f'Tracks for artist: {artist_id} {artist_name} went from {orig_num_tracks} to '
              f'{len(artist_tracks_doc)}')
        # write even if nothing changed, so that the mtime updates
        artist_tracks_doc.write(force=True)

        return

    def _get_album_tracks(self, album_id, album_name):
        # TODO there is no way to force a refresh here; add something?
        album_tracks_doc = self._get_doc('album-tracks', album_id, album_name)

        if album_tracks_doc.exists():
            return album_tracks_doc

        print(f'Fetching tracks for album: {album_id} {album_name}...', end='')

        album_tracks = djlib_config.spotify.get_album_tracks(album_id)

        album_tracks_doc.set_df(album_tracks)
        album_tracks_doc.write()
        print(f' {len(album_tracks_doc)} tracks')

        return album_tracks_doc


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



    def refresh_artist(self, artist_id=None, artist_name=None, refresh_days=30,
                       force=False,
                       force_albums=False,
                       force_tracks=False):
        """Refreshes the track database for the specified artist."""

        artist_id, artist_name = self._artist_id_and_name(artist_id, artist_name)

        self._refresh_artist_albums(artist_id, artist_name, refresh_days=refresh_days,
                                    force=(force or force_albums))
        self._refresh_artist_tracks(artist_id, artist_name,
                                    force=(force or force_tracks))

        return




    def get_artist_discography(self, *,
                               artist_id=None,
                               artist_name=None,
                               deduplicate_tracks=False):
        """
        Returns all known tracks for the artist. At least one of (artist_id, artist_name)
        must be specified.
        Note that this relies on the current track database on disk. To get the latest tracks,
        call refresh_artist() first.
           deduplicate_tracks (default False): try to filter out duplicate releases of the same track
                                               (potentially expensive)
        """

        artist_id, artist_name = self._artist_id_and_name(artist_id, artist_name)

        artist_tracks = self._get_doc('artist-tracks', artist_id, artist_name).get_df()

        if len(artist_tracks) > 0 and deduplicate_tracks:
            artist_tracks = self._deduplicate_tracks(artist_tracks)

        return artist_tracks


def get_instance():
    global _singleton
    if _singleton is None:
        _singleton = _SpotifyDiscography()
    return _singleton

