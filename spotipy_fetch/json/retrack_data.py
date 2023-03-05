import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import json
from mongo_format import *

# Current your account to fetch data. These info should be private.
def create_spotipy():
    client_id = "5288959a7fcf4531bddf261d0b010485"
    client_secret = "bdccb523f6d044ecb06fe698f9fc0391"

    client_credentials_manager = SpotifyClientCredentials(
        client_id=client_id, client_secret=client_secret
    )
    stf = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    return stf


def get_artist_idx():
    with open("untracked_artist_idxs.json", "r", encoding="utf-8") as f:
        artists_idxs = json.load(f)

    artists_limit = 50
    partition_artists_idxs = [
        artists_idxs[i * artists_limit : (i + 1) * artists_limit]
        for i in range(-(-len(artists_idxs) // artists_limit))
    ]
    print(len(partition_artists_idxs), "pages in total.")

    stf = create_spotipy()
    artists_objs = []
    for count, partition in enumerate(partition_artists_idxs):
        print(count, "start... ", end="")
        fetch_result = stf.artists(partition)["artists"]
        for artist in fetch_result:
            # print(artist)
            fields_name_changes = [
                ("genres", "artist_genres"),
                ("id", "artist_spotify_idx"),
                ("name", "artist_name"),
                ("popularity", "artist_popularity"),
            ]
            simple_artist = {
                new_key: artist[old_key] for old_key, new_key in fields_name_changes
            }
            # print(simple_artist)
            artists_objs.append(simple_artist)
        print(count, "end!")

    with open("tb_add_artists.json", "w", encoding="utf-8") as f:
        json.dump(artists_objs, f, ensure_ascii=False)
    with open("untracked_artist_idxs.json", "w", encoding="utf-8") as f:
        json.dump([], f, ensure_ascii=False)  # clear everything


if __name__ == "__main__":
    # get_artist_idx()
    db = MongoDB()
    db.insert_mongo("tb_add_artists.json", "artists")
    db.clean_duplicates_mongo("artists")