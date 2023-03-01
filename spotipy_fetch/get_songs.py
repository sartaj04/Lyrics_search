import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd

# Current your account to fetch data. These info should be private.
def create_spotipy():
    client_id = ""
    client_secret = ""

    client_credentials_manager = SpotifyClientCredentials(
        client_id=client_id, client_secret=client_secret
    )
    stf = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    return stf


def get_songs(artist_page=0, album_page=0, end_page=20):
    for page in range(album_page, end_page + 1):
        print("Now handling album page:", page)
        df = pd.read_csv(
            f"./album_dataset/album_id_spotipy{artist_page:02d}_{page:02d}.csv"
        )
        album_idxs = list(df["album_idx"])
        album_idxs = sorted(list(set(album_idxs)))

        album_limit = 20
        partition_albums_idxs = [
            album_idxs[i * album_limit : (i + 1) * album_limit]
            for i in range(-(-len(album_idxs) // album_limit))
        ]

        stf = create_spotipy()
        track_results = []
        for count, partition in enumerate(partition_albums_idxs):
            print(count, "start... ", end="")
            tmp_track_results = []
            stf_albums = stf.albums(partition)["albums"]
            for stf_album in stf_albums:
                tracks = stf_album["tracks"]["items"]

                for track in tracks:
                    tmp_track_results.append(
                        {
                            "artists_idxs": [i["id"] for i in track["artists"]],
                            "duration": round(track["duration_ms"], 1000),
                            "explicit": track["explicit"],
                            "track_idx": track["id"],
                            "track_name": track["name"],
                        }
                    )
            # print(len(tmp_track_results))
            # get features
            track_limit = 50
            partition_tracks = [
                tmp_track_results[i * track_limit : (i + 1) * track_limit]
                for i in range(-(-len(tmp_track_results) // track_limit))
            ]
            # print("1: ", [len(partition_tracks[i]) for i in range(len(partition_tracks))])

            for tracks in partition_tracks:
                tracks_idxs = [t["track_idx"] for t in tracks]
                track_len = len(tracks_idxs)
                tracks_features = stf.audio_features(tracks_idxs)  # ["audio_features"]
                # print("2: ", min(track_len, track_limit))
                for j in range(min(track_len, track_limit)):
                    if tracks_features[j]:  # not None
                        track_dict = tracks[j]
                        track_dict["danceability"] = tracks_features[j]["danceability"]
                        track_dict["energy"] = tracks_features[j]["energy"]
                        track_dict["loudness"] = tracks_features[j]["loudness"]
                        track_dict["speechiness"] = tracks_features[j]["speechiness"]
                        track_dict["acousticness"] = tracks_features[j]["acousticness"]
                        track_dict["instrumentalness"] = tracks_features[j][
                            "instrumentalness"
                        ]
                        track_dict["liveness"] = tracks_features[j]["liveness"]
                        track_dict["valence"] = tracks_features[j]["valence"]
                        track_dict["tempo"] = round(tracks_features[j]["tempo"], 0)

                track_results.extend(tracks)
                # print("3: ", len(track_results))

            print(count, "end!")
        pd.DataFrame(track_results).to_csv(
            f"./song_dataset/track_id_spotipy{artist_page:02d}_{page:02d}.csv",
            index=False,
        )


# TODO: specify the pages
get_songs(4)
