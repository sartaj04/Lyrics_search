import json
from Mongo_Collection import MongoCollection
from utils import object_id_to_str


def get_datasets_intersection():
    col = MongoCollection()
    x = list(col.col.find({"lyrics": {"$ne": None}}))
    x = [object_id_to_str(i) for i in x]
    with open("tracks_intersection.json", "w", encoding="utf-8") as f:
        json.dump(x, f, ensure_ascii=False)


def tracks_to_artists_albums():
    with open("tracks_intersection.json", "r", encoding="utf-8") as f:
        tracks = json.load(f)

    albums_dict = {}
    artist_dict = {}
    for track in tracks:
        album = track["album"]
        artists = album["artists"] + track["artists"]

        # album add
        if album["_id"] not in albums_dict:
            albums_dict[album["_id"]] = album

        # artists add
        for artist in artists:
            if artist["_id"] not in artist_dict:
                artist_dict[artist["_id"]] = artist

    with open("albums_small.json", "w", encoding="utf-8") as f:
        json.dump(list(albums_dict.values()), f, ensure_ascii=False)
    with open("artists_small.json", "w", encoding="utf-8") as f:
        json.dump(list(artist_dict.values()), f, ensure_ascii=False)


if __name__ == "__main__":
    # get_datasets_intersection()
    tracks_to_artists_albums()