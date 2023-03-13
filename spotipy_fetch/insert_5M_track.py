import csv
from utils import get_dataframe, find_files
from Mongo_Collection import MongoCollection
from retrack_data import create_spotipy
import json
import pandas as pd


###TODO: Complete this python file

spotipy = create_spotipy(
    "0c525f920ffa4e6fbb3f538a4ece013f", "284dc2899d2844cbbc952e586c6959c5"
)


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


def artists_datasets_common():
    pass


# def clean_df(df):
#     df = df[~(df["lyrics"] == "[Instrumental]")]
#     df = df[~(df["lyrics"] == "TBD")]
#     df = df[~(df["artist"].str.contains("&"))]
#     return df


def split_df():
    page_num = 985569
    # Handle page 0 without skiprows
    for i in range(6):
        genius_artist_df = pd.read_csv(
            "../../dataset/ds2.csv",
            usecols=["title", "artist", "lyrics", "id"],
            header=0,
            skiprows=(range(1, page_num * i)) if i > 0 else None,
            nrows=page_num,
        )
        genius_artist_df.to_csv(f"extra_track_{i}.csv", index=False)


def insert_genius_data(page=0, subpage=0):
    # with open(f"extra_track_{page}.csv") as f:
    #     reader = csv.DictReader(f)

    #     load_limit = 25000  # max api call per day
    #     album_idxs = list(reader)

    #     partition_albums_idxs = [
    #         album_idxs[i * album_limit : (i + 1) * album_limit]
    #         for i in range(-(-len(album_idxs) // album_limit))
    #     ]

    # artists_dict = {}
    # albums_dict = {}
    # tracks_dict = {}
    # for track in tracks:
    #     query = f"track:{track['title']} artist:{track['artist']}"
    #     print(query)
    #     search_res = spotipy.search(q=query, type="track", offset=0, limit=1)
    #     search_items = search_res["tracks"]["items"]
    #     if len(search_items) > 0:
    #         spotify_track_info = search_items[0]
    #         spotify_artists = spotify_track_info["artists"]
    #         spotify_track_name = spotify_track_info["name"]

    #         # ensure same track title and same artist
    #         is_same_track = is_substring(spotify_track_name, track["title"])
    #         contains_same_artist = any(
    #             [
    #                 is_substring(artist["name"], track["artist"])
    #                 for artist in spotify_artists
    #             ]
    #         )
    #         print(is_same_track, contains_same_artist)
    #         if is_same_track and contains_same_artist:
    #             pass
    #     break
    pass


if __name__ == "__main__":
    # split_df()
    insert_genius_data(0)