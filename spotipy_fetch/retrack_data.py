import json
from utils import create_spotipy

# TODO
client_id = ""
client_secret = ""


def get_artist_idx():
    with open("untracked_artist_idxs.json", "r", encoding="utf-8") as f:
        artists_idxs = json.load(f)

    artists_limit = 50
    partition_artists_idxs = [
        artists_idxs[i * artists_limit : (i + 1) * artists_limit]
        for i in range(-(-len(artists_idxs) // artists_limit))
    ]
    print(len(partition_artists_idxs), "pages in total.")

    stf = create_spotipy(client_id, client_secret)
    artists_objs = []
    for count, partition in enumerate(partition_artists_idxs):
        print(count, "start... ", end="")
        fetch_result = stf.artists(partition)["artists"]
        for artist in fetch_result:
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
    get_artist_idx()