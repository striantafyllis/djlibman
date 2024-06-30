
from utils import *


def manage_consider_playlist(ctx, up_to_track, source_playlist='consider', target_playlist='to buy'):
    playlists = ctx.spotify.get_playlists()

    source_playlist_id = playlists.loc[source_playlist, 'id']
    target_playlist_id = playlists.loc[target_playlist, 'id']

    sys.stdout.write("Getting tracks of playlist '%s'... " % source_playlist)
    sys.stdout.flush()
    source_playlist_tracks = ctx.spotify.get_playlist_tracks(source_playlist_id)
    sys.stdout.write("%d tracks\n" % len(source_playlist_tracks))
    sys.stdout.flush()

    sys.stdout.write("Getting liked tracks... ")
    sys.stdout.flush()
    liked_tracks = ctx.spotify.get_liked_tracks()
    sys.stdout.write("%d tracks\n" % len(liked_tracks))
    sys.stdout.flush()

    if isinstance(up_to_track, int):
        up_to = up_to_track
    else:
        # is there no better way to do this in Pandas?
        up_to = None
        for i in range(len(source_playlist_tracks)):
            if up_to_track in source_playlist_tracks.name.iat[i]:
                up_to = i+1
                break
        if up_to is None:
            raise Exception("Track '%s' not found in playlist '%s'" % (up_to_track, source_playlist))

    listened_track_ids = source_playlist_tracks.index[:up_to]
    liked_listened_track_ids = listened_track_ids.intersection(liked_tracks.index)

    print("The following tracks will be removed from playlist '%s':" % (source_playlist))
    pretty_print_tracks(source_playlist_tracks.loc[listened_track_ids], indent='    ', enum=True)

    print()
    print("Of these, the following tracks will be added to playlist '%s':" % target_playlist)
    pretty_print_tracks(source_playlist_tracks.loc[liked_listened_track_ids], indent='    ', enum=True)
    print()

    choice = get_user_choice('Proceed?')
    if choice == 'yes':
        print("Adding %d tracks to playlist '%s'" % (len(liked_listened_track_ids), target_playlist))
        ctx.spotify.add_tracks_to_playlist(target_playlist_id, liked_listened_track_ids)

        print("Removing %d tracks from playlist '%s'" % (len(listened_track_ids), source_playlist))
        ctx.spotify.remove_tracks_from_playlist(source_playlist_id, listened_track_ids)

    return
