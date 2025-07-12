
import pandas as pd
import numpy as np

_DOWNTEMPO_CUTOFF = 112

def track_is(
        track,
        *,
        danceable=None,
        ambient=None,
        song=None,
        uptempo=None,
        classes=None,
        not_classes=None,
        flavors=None,
        not_flavors=None,
        before=None,
        after=None
):
    if danceable is not None:
        if pd.isna(track['Danceable']):
            return False
        if bool(danceable) != bool(track['Danceable']):
            return False

    if ambient is not None:
        if pd.isna(track['Ambient']):
            return False
        if bool(ambient) != bool(track['Ambient']):
            return False

    if song is not None:
        if pd.isna(track['Song']):
            return False
        if bool(song) != bool(track['Song']):
            return False

    if uptempo is not None:
        if pd.isna(track['BPM']):
            return False
        if not isinstance(track['BPM'], float):
            raise ValueError('BPM must be float')
        if bool(uptempo) != (track['BPM'] >= _DOWNTEMPO_CUTOFF):
            return False

    if classes is not None:
        if pd.isna(track['Class']):
            return False
        track_class = track['Class']
        found = False
        for clss in classes:
            if track_class.startswith(clss):
                found = True
                break
        if not found:
            return False

    if not_classes is not None:
        if not pd.isna(track['Class']):
            track_class = track['Class']
            found = False
            for clss in not_classes:
                if track_class.startswith(clss):
                    found = True
                    break
            if found:
                return False

    if flavors is not None:
        if not isinstance(track['Flavors'], list) and pd.isna(track['Flavors']):
            return False
        track_flavors = [f.strip().upper() for f in track['Flavors']]
        found = False
        for flavor in flavors:
            if flavor.upper() in track_flavors:
                found = True
                break
        if not found:
            return False

    if not_flavors is not None:
        if isinstance(track['Flavors'], list):
            track_flavors = [f.strip().upper() for f in track['Flavors']]
            found = False
            for flavor in not_flavors:
                if flavor.upper() in track_flavors:
                    found = True
                    break
            if found:
                return False

    if before is not None:
        if not isinstance(before, pd.Timestamp):
            before = pd.to_datetime(before, utc=True)

        if pd.isna(track['Date Added']) or track['Date Added'] >= before:
            return False

    if after is not None:
        if not isinstance(after, pd.Timestamp):
            after = pd.to_datetime(before, utc=True)

        if pd.isna(track['Date Added']) or track['Date Added'] <= after:
            return False

    return True

def filter_tracks(
        tracks, **kwargs
):
    return tracks.loc[
        tracks.apply(
            lambda track: track_is(track, **kwargs),
            axis=1
        )
    ]

def classify_tracks(tracks):
    flavor_groupings = {
        'Progressive': ['Progressive'],
        'Organic': ['Organic'],
        'Afro/Latin/Funky': ['Afro', 'Latin', 'Funky'],
        'Organic/Afro/Latin/Funky': ['Organic', 'Afro', 'Latin', 'Funky'],
    }

    covered_flavors = set([
        flavor for flavors in flavor_groupings.values() for flavor in flavors
    ])

    prime_class_groupings = {
        'A': ['A'],
        'B': ['B'],
        'AB': ['A', 'B'],
        'C': ['C']
    }

    other_class_groupings = {
        'D': ['D'],
        'X': ['X'],
        'Pending': ['?']
    }

    playlists = []

    for uptempo in [True, False]:
        rb_prefix = ['Downtempo'] if not uptempo else []

        for flavor_name, flavors in flavor_groupings.items():
            for class_name, classes in prime_class_groupings.items():
                playlists.append({
                    'rekordbox_name': rb_prefix + [f'{flavor_name} {class_name}'],
                    'spotify_name': f'{flavor_name} {class_name}'
                        if uptempo and class_name in ['A', 'B', 'AB'] else None,
                    'kwargs': {
                        'uptempo': uptempo,
                        'flavors': flavors,
                        'classes': classes
                    }
                })

        for class_name, classes in prime_class_groupings.items():
            playlists.append({
                'rekordbox_name': rb_prefix + [f'Other {class_name}'],
                'kwargs': {
                    'uptempo': uptempo,
                    'not_flavors': covered_flavors,
                    'classes': classes
                }
            })

        for class_name, classes in other_class_groupings.items():
            for flavor_name, flavors in flavor_groupings.items():
                playlists.append({
                    'rekordbox_name': rb_prefix + [class_name, flavor_name],
                    'kwargs': {
                        'uptempo': uptempo,
                        'flavors': flavors,
                        'classes': classes
                    }
                })

            playlists.append({
                'rekordbox_name': rb_prefix + [class_name, 'Other'],
                'kwargs': {
                    'uptempo': uptempo,
                    'not_flavors': covered_flavors,
                    'classes': classes
                }
            })

    for playlist in playlists:
        playlist['tracks'] = filter_tracks(tracks, **(playlist['kwargs']))

    nonempty_playlists = [
        playlist for playlist in playlists if len(playlist['tracks']) > 0
    ]

    return nonempty_playlists





