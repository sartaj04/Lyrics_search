import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import csv

artist_limit = 250

# Current your account to fetch data. These info should be private.
def create_spotipy():
    client_id = "8aa464cad6524f3bb664c009125bc1e3"
    client_secret = "41a8b8604d3f41079e9303a937f108ee"

    client_credentials_manager = SpotifyClientCredentials(
        client_id=client_id, client_secret=client_secret
    )
    stf = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    return stf


import re


def get_ymd(date):
    ymd_list = [None] * 3
    if len(date) == 0:
        return ymd_list
    ymd = date.split("-")
    len_ymd = len(ymd)
    for i in range(3):
        if len_ymd <= i:
            ymd_list[i] = None
        else:
            digit_str = re.sub("\D", "", ymd[i])
            ymd_list[i] = int(digit_str) if len(digit_str) > 0 else None
            ymd_list[i] = None if ymd_list[i] == 0 else ymd_list[i]
    return ymd_list


def get_albums(artist_page=0, album_page=0):
    df = pd.read_csv(f"artist_id_spotipy{artist_page:02d}.csv")
    artist_idxs = list(df["artist_id"])
    artist_idxs = sorted(list(set(artist_idxs)))
    partition_artist_idxs = [
        artist_idxs[i * artist_limit : (i + 1) * artist_limit]
        for i in range((len(artist_idxs) + artist_limit - 1) // artist_limit)
    ][album_page:]

    stf = create_spotipy()
    for count, partition in enumerate(partition_artist_idxs):
        print(count, "start... ", end="")
        album_results = []
        for artist_id in partition:
            uri = f"spotify:artist:{artist_id}"
            results = stf.artist_albums(uri, album_type="album")
            albums = results["items"]
            while results["next"]:
                results = stf.next(results)
                albums.extend(results["items"])

            for album in albums:
                year, month, day = get_ymd(album["release_date"])
                album_results.append(
                    {
                        "artists_idxs": [i["id"] for i in album["artists"]],
                        "album_idx": album["id"],
                        "album_name": album["name"],
                        "album_release_year": year,
                        "album_release_month": month,
                        "album_release_day": day,
                    }
                )
        print(count, "end!")

        pd.DataFrame(album_results).to_csv(
            f"album_id_spotipy{artist_page:02d}_{count + album_page:02d}.csv",
            index=False,
        )


# TODO: specify the pages
get_albums(0, 0)
