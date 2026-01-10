
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
        'Salsa': ['Salsa'],
    }

    covered_flavors = set([
        flavor for flavors in flavor_groupings.values() for flavor in flavors
    ])

    prime_class_groupings = {
        'A': ['A'],
        'B': ['B'],
        'AB': ['A', 'B'],
    }

    other_class_groupings = {
        'C': ['C'],
        'D': ['D'],
        'X': ['X'],
        'Pending': ['?']
    }

    playlists = []

    for flavor_name, flavors in flavor_groupings.items():
        for uptempo in [True, False]:
            uptempo_relevant = flavor_name in ['Progressive', 'Organic', 'Afro', 'Latin', 'Funky']

            if not uptempo_relevant and not uptempo:
                # we've already handled everything in True...
                continue

            rb_prefix = ['Downtempo'] if (uptempo_relevant and not uptempo) else []

            for class_name, classes in (prime_class_groupings | other_class_groupings).items():
                rekordbox_names = [['managed'] + rb_prefix + [f'{flavor_name} {class_name}']]

                if (uptempo or not uptempo_relevant) and class_name in prime_class_groupings:
                    rekordbox_names += [['managed AB'] + rb_prefix + [f'{flavor_name} {class_name}']]
                    spotify_name = f'DJ {flavor_name} {class_name}'
                else:
                    spotify_name = None

                playlists.append({
                    'rekordbox_names': rekordbox_names,
                    'spotify_name': spotify_name,
                    'kwargs': {
                        'uptempo': uptempo if uptempo_relevant else None,
                        'flavors': flavors,
                        'classes': classes
                    }
                })

    for class_name, classes in prime_class_groupings.items():
        playlists.append({
            'rekordbox_names': [['managed'] + rb_prefix + [f'Other {class_name}']],
            'kwargs': {
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





