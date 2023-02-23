import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import csv

page_limit = 2500

# Current your account to fetch data. These info should be private.
def create_spotipy():
    client_id = ""
    client_secret = ""

    client_credentials_manager = SpotifyClientCredentials(
        client_id=client_id, client_secret=client_secret
    )
    stf = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    return stf


def get_artist(page=0):
    stf = create_spotipy()
    steam = open(f"../dataset/dfArtistDetailed.csv", "r", newline="", encoding="utf-8")
    reader = csv.reader(steam, delimiter=",")
    next(reader, None)

    search_artists = list(reader)

    start = page * page_limit
    end = start + page_limit + 1

    track_results = []
    for art in search_artists[start:end]:
        query = f"remaster artist:{art[2]}".replace(" ", "%20")
        track_results.append(stf.search(q=query, type="artist", offset=0, limit=3))

    organized_data = [
        res["artists"]["items"]
        for res in track_results
        if len(res["artists"]["items"]) > 0
    ]
    flatten_data = [j for sub in organized_data for j in sub]
    organized_artists = [
        {
            "artist_name": item["name"],
            "artist_id": item["id"],
            "artist_popularity": item["popularity"],
            "artist_genres": item["genres"],
        }
        for item in flatten_data
    ]
    pd.DataFrame(organized_artists).to_csv(
        f"artist_id_spotipy{page:02d}.csv", index=False
    )


# TODO: specify the page
get_artist(0)
