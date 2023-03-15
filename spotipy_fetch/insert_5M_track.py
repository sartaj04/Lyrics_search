from utils import create_spotipy, get_ymd
from Mongo_Collection import MongoCollection
import json
import pandas as pd

# TODO: insert your client tokens here
client_id = "8aa464cad6524f3bb664c009125bc1e3"
client_secret = "41a8b8604d3f41079e9303a937f108ee"


def get_5M_artists():
    # Generate list of artists in 5M Dataset
    genius_artist_df = pd.read_csv("../dataset/ds2.csv", usecols=["artist"], header=0)
    unique_artist_df = genius_artist_df["artist"].unique()
    extra_search_artists = [a.strip() for a in unique_artist_df]
    export_df = pd.DataFrame(extra_search_artists)
    export_df.to_csv("5M_artists.csv", index=False)

    col = MongoCollection("mongodb://34.171.116.112:27017/", "lyricsDataset", "artists")
    db_artists = col.col.find({}, {"artist_name": 1})
    db_artist_df = pd.DataFrame(list(db_artists))
    unique_artist_df2 = pd.DataFrame(unique_artist_df, columns=["artist"])
    df_all = unique_artist_df2.merge(
        db_artist_df,
        left_on="artist",
        right_on="artist_name",
        how="left",
        indicator=True,
    )
    unfound_artists = df_all[df_all["_merge"] == "left_only"]
    unfound_artists["artist"].to_csv("5M_unfound_artists.csv", index=False)

    found_artists = df_all[df_all["_merge"] == "both"]
    found_artists.drop(["artist_name", "_merge"], axis=1).to_csv(
        "5M_found_artists.csv", index=False
    )


def clean_df():
    genius_df = pd.read_csv("../dataset/ds2.csv", header=0)
    genius_df_nonan = genius_df[genius_df["lyrics"].notna()]
    gdf = genius_df_nonan[genius_df_nonan["lyrics"].map(lambda x: x.isascii())]
    gdf.to_csv("../dataset/ds2_ENGLISH.csv", index=False)


def get_basic_track_info(page=0, display_404=False):
    page_limit = 1000 + 200 * page
    rows_skip = 100 * page * (page + 9)
    stf = create_spotipy(client_id, client_secret)
    artists_df = pd.read_csv(
        "5M_artists_ENGLISH.csv",
        sep=",",
        header=None,
        skiprows=rows_skip,
        nrows=page_limit,
    )
    artists = list(artists_df[0])

    # Export dictionary
    artist_dict = {}
    album_dict = {}
    track_dict = {}

    # insert each track info into 3 dictionaries
    def add_track_info(target_artist, stf_track):
        # check if artist name is we want
        if stf_track == None:
            return False
        stf_album_artists = [a["name"].lower() for a in stf_track["artists"]]
        # punctuation unmatched handling
        target_artist = target_artist.replace("'", "’")  # Genius -> Spotify

        if target_artist.lower() not in stf_album_artists:
            # if return_found_err:
            #     print(f"Not match for {target_artist} in {stf_album_artists}")
            return False

        stf_album = stf_track["album"]
        stf_artists = stf_album["artists"] + stf_track["artists"]

        # artist add
        for stf_artist in stf_artists:
            artist_stf_idx = stf_artist["id"]
            if artist_stf_idx not in artist_dict:
                # artist formatting
                db_artist = {
                    "artist_name": stf_artist["name"],
                    "artist_id": artist_stf_idx,
                    "artist_popularity": None,  # popularity not included
                    "artist_genres": None,  # genres not included
                }
                artist_dict[artist_stf_idx] = db_artist

        # album add
        album_stf_idx = stf_album["id"]
        if album_stf_idx not in album_dict:
            # album formatting
            year, month, day = get_ymd(stf_album["release_date"])
            db_album = {
                "artists": [artist_dict[a["id"]] for a in stf_album["artists"]],
                "album_idx": album_stf_idx,
                "album_name": stf_album["name"],
                "album_release_year": year,
                "album_release_month": month,
                "album_release_day": day,
            }
            album_dict[album_stf_idx] = db_album

        # track add
        track_stf_idx = stf_track["id"]
        if track_stf_idx not in track_dict:
            # track formatting
            db_track = {
                "artists": [artist_dict[a["id"]] for a in stf_track["artists"]],
                "album": album_dict[album_stf_idx],
                "duration": int(round(stf_track["duration_ms"] // 1000, 0)),
                "explicit": stf_track["explicit"],
                "track_spotify_idx": stf_track["id"],
                "track_name": stf_track["name"],
                "lyrics": None,
            }
            track_dict[track_stf_idx] = db_track

        return True

    for idx, artist in enumerate(artists):
        found_result = stf.search(
            q=f"artist:{artist}", type="track", offset=0, limit=50
        )["tracks"]
        # get all tracks available in Spotify
        tracks_result = found_result["items"]
        if tracks_result:
            artist_found = [
                add_track_info(artist, stf_track) for stf_track in tracks_result
            ]
            correct_artist = any(artist_found)
            if display_404 and all(artist_found) == False:
                print(
                    f"({idx}, {artist}) is not found in first 50 records."
                )  # for debug
        else:
            correct_artist = False

        # If not match then stop wasting api call
        while correct_artist and found_result["next"]:
            found_result = stf.next(found_result)["tracks"]
            tracks_result = found_result["items"]
            if tracks_result:
                correct_artist = any(
                    [add_track_info(artist, stf_track) for stf_track in tracks_result]
                )
            else:
                correct_artist = False

    structured_tracks = list(track_dict.values())
    output_tracks = []
    # Also include audio features
    track_limit = 50
    partition_tracks = [
        structured_tracks[i * track_limit : (i + 1) * track_limit]
        for i in range(-(-len(structured_tracks) // track_limit))
    ]

    for tracks in partition_tracks:
        tracks_idxs = [t["track_spotify_idx"] for t in tracks]
        track_len = len(tracks_idxs)
        tracks_features = stf.audio_features(tracks_idxs)  # ["audio_features"]
        for j in range(min(track_len, track_limit)):
            if tracks_features[j]:  # not None
                track_dict = tracks[j]
                track_dict["danceability"] = tracks_features[j]["danceability"]
                track_dict["energy"] = tracks_features[j]["energy"]
                track_dict["loudness"] = tracks_features[j]["loudness"]
                track_dict["speechiness"] = tracks_features[j]["speechiness"]
                track_dict["acousticness"] = tracks_features[j]["acousticness"]
                track_dict["instrumentalness"] = tracks_features[j]["instrumentalness"]
                track_dict["liveness"] = tracks_features[j]["liveness"]
                track_dict["valence"] = tracks_features[j]["valence"]
                track_dict["tempo"] = round(tracks_features[j]["tempo"], 0)

        output_tracks.extend(tracks)

    with open(
        f"extra_track_dataset/track_data_{page:02d}.json", "w", encoding="utf-8"
    ) as f:
        json.dump(output_tracks, f, ensure_ascii=False)


def get_basic_track_infos(start_page=0, end_page=57):
    for i in range(start_page, end_page + 1):
        get_basic_track_info(i)


def merge_with_lyrics(page=0):

    df_5M = pd.read_csv(
        "../dataset/ds2_ENGLISH.csv",
        usecols=["title", "artist", "lyrics"],
        header=0,
    )

    df_tracks = pd.read_json(f"extra_track_dataset/track_data_{page:02d}.json")
    df_tracks = df_tracks.drop_duplicates()

    # TODO
    merged_df = df_tracks.merge(
        df_5M, how="left", left_on="artist", right_on="artist_name"
    )
    search_df = merged_df[merged_df["artist_name"].notnull()]


if __name__ == "__main__":
    # for single one
    get_basic_track_info(0)
    # for multiple pages
    # get_basic_track_infos(0, 0)