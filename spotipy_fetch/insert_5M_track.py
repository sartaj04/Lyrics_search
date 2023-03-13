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


def clean_df(df):
    df = df[~(df["lyrics"] == "[Instrumental]")]
    df = df[~(df["lyrics"] == "TBD")]
    df = df[~(df["artist"].str.contains("&"))]
    return df


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


def is_substring(str1, str2):
    return (str1 in str2) or (str2 in str1)


def insert_genius_data(page=0, subpage=0):
    with open(f"extra_track_{page}.csv") as f:
        reader = csv.DictReader(f)

        load_limit = 25000  # max api call per day
        album_idxs = list(reader)

        partition_albums_idxs = [
            album_idxs[i * album_limit : (i + 1) * album_limit]
            for i in range(-(-len(album_idxs) // album_limit))
        ]

    artists_dict = {}
    albums_dict = {}
    tracks_dict = {}
    for track in tracks:
        query = f"track:{track['title']} artist:{track['artist']}"
        print(query)
        search_res = spotipy.search(q=query, type="track", offset=0, limit=1)
        search_items = search_res["tracks"]["items"]
        if len(search_items) > 0:
            spotify_track_info = search_items[0]
            spotify_artists = spotify_track_info["artists"]
            spotify_track_name = spotify_track_info["name"]

            # ensure same track title and same artist
            is_same_track = is_substring(spotify_track_name, track["title"])
            contains_same_artist = any(
                [
                    is_substring(artist["name"], track["artist"])
                    for artist in spotify_artists
                ]
            )
            print(is_same_track, contains_same_artist)
            if is_same_track and contains_same_artist:
                pass
        break


if __name__ == "__main__":
    # split_df()
    insert_genius_data(0)