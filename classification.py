
import pandas as pd
import numpy as np

_DOWNTEMPO_CUTOFF = 112

flavor_groupings = {
    'Progressive': ['Progressive'],
    'Progressive-Adjacent': ['Progressive-Adjacent'],
    'Afro/Latin/Funky': ['Afro', 'Latin', 'Funky'],
    'Organic': ['Organic'],
    'Other': None
}

class_groupings = {
    '': ['A', 'B'],
    'Pending': ['?'],
    'Old': ['O'],
    'CX': None
}

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

def _join_names(name1, name2):
    if not isinstance(name1, tuple):
        if name1 == '':
            name1 = ()
        else:
            name1 = (name1,)

    if not isinstance(name2, tuple):
        if name2 == '':
            name2 = ()
        else:
            name2 = (name2,)

    return name1 + name2

def classify_by_flavor(name, tracks):
    groups = {}

    for flavor_name, flavor_group in flavor_groupings.items():
        if flavor_group is None:
            other_flavors = []
            for other_flavor_group in flavor_groupings.values():
                if other_flavor_group is not None:
                    other_flavors += other_flavor_group

            flavor_tracks = filter_tracks(tracks, not_flavors=other_flavors)
        else:
            flavor_tracks = filter_tracks(tracks, flavors=flavor_group)

        groups[_join_names(name, flavor_name)] = flavor_tracks

    return groups


def classify_by_class(name, tracks):
    groups = {}

    for class_name, class_group in class_groupings.items():
        if class_group is None:
            other_classes = []
            for other_class_group in class_groupings.values():
                if other_class_group is not None:
                    other_classes += other_class_group

            class_tracks = filter_tracks(tracks, not_classes=other_classes)
        else:
            class_tracks = filter_tracks(tracks, classes=class_group)

        groups[_join_names(name, class_name)] = class_tracks

    return groups

def classify_by_danceability(name, tracks):
    danceable_tracks = filter_tracks(tracks, danceable=True)
    ambient_tracks = filter_tracks(tracks, ambient=True)

    return {
        _join_names(name, ''): danceable_tracks,
        _join_names(name, 'Ambient'): ambient_tracks
    }

def classify_by_tempo(name, tracks):
    uptempo_tracks = filter_tracks(tracks, uptempo=True)
    downtempo_tracks = filter_tracks(tracks, uptempo=False)

    return {
        _join_names(name, ''): uptempo_tracks,
        _join_names(name, 'Downtempo'): downtempo_tracks
    }


def classify_tracks(tracks):
    groups1 = classify_by_class((), tracks)

    groups2 = {}
    for name, group in groups1.items():
        groups2.update(classify_by_danceability(name, group))

    groups3 = {}
    for name, group in groups2.items():
        groups3.update(classify_by_tempo(name, group))

    groups4 = {}
    for name, group in groups3.items():
        groups4.update(classify_by_flavor(name, group))

    groups = groups4

    # filter out empty groups
    nonempty_groups = {
        name: group
        for name, group in groups.items()
        if len(group) > 0
    }

    return nonempty_groups
