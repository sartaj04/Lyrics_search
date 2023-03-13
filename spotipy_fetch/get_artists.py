import pandas as pd
import csv
from utils import create_spotipy

page_limit = 2500


# TODO
client_id = "0c525f920ffa4e6fbb3f538a4ece013f"
client_secret = "284dc2899d2844cbbc952e586c6959c5"


def get_artist(page=0):
    stf = create_spotipy(client_id, client_secret)
    steam = open(f"../dataset/dfArtistDetailed.csv", "r", newline="", encoding="utf-8")
    reader = csv.reader(steam, delimiter=",")
    next(reader, None)

    search_artists = list(reader)

    start = page * page_limit
    end = start + page_limit + 1

    track_results = []
    for art in search_artists[start:end]:
        query = f"artist:{art[2]}".replace(" ", "%20")
        track_results.append(stf.search(q=query, type="artist", offset=0, limit=1))

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
        f"./artist_dataset/artist_id_spotipy{page:02d}.csv", index=False
    )


# TODO: specify the page
get_artist(0)
