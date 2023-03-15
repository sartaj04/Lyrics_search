import json
from Mongo_Collection import MongoCollection
from utils import object_id_to_str


def get_datasets_intersection():
    col = MongoCollection()
    x = list(col.col.find({"lyrics": {"$ne": None}}))
    x = [object_id_to_str(i) for i in x]
    with open("tracks_intersection.json", "w", encoding="utf-8") as f:
        json.dump(x, f, ensure_ascii=False)


def tracks_to_artists_albums(
    track_dir="tracks_intersection.json",
    album_export_dir="albums_small.json",
    artist_export_dir="artists_small.json",
):
    with open(track_dir, "r", encoding="utf-8") as f:
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

    with open(album_export_dir, "w", encoding="utf-8") as f:
        json.dump(list(albums_dict.values()), f, ensure_ascii=False)
    with open(artist_export_dir, "w", encoding="utf-8") as f:
        json.dump(list(artist_dict.values()), f, ensure_ascii=False)


if __name__ == "__main__":
    start_page, end_page = 0, 0
    track_dir = f"track_extra_dataset/track_data_{start_page:02d}.json"
    tracks_to_artists_albums()