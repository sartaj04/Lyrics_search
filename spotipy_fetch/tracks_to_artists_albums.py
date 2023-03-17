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
    track_export_dir="",
):
    with open(track_dir, "r", encoding="utf-8") as f:
        tracks = json.load(f)

    # Search if exists in DB
    print(f"Tracks number before: {len(tracks)}")
    print("Search MongoDB...")
    tracks_col = MongoCollection(database="trackInfo")
    insert_lyrics_tracks = []
    for track in tracks:
        while True:
            print(track["track_spotify_idx"])
            try:
                mongo_tracks = list(
                    tracks_col.col.find(
                        {"track_spotify_idx": track["track_spotify_idx"]}, {"lyrics": 1}
                    )
                )
                break
            except Exception as e:
                print(e)

        # insert data if tracks not found
        if len(mongo_tracks) == 0:
            insert_lyrics_tracks.append(track)

    print("Creating artists and artists info...")
    albums_dict = {}
    artist_dict = {}
    for track in insert_lyrics_tracks:
        # formatting bug fix
        track["album"]["album_spotify_idx"] = track["album"]["album_idx"]
        del track["album"]["album_idx"]
        for artist in track["album"]["artists"]:
            artist["artist_spotify_idx"] = artist["artist_id"]
            del artist["artist_id"]
        for artist in track["artists"]:
            artist["artist_spotify_idx"] = artist["artist_id"]
            del artist["artist_id"]

        album = track["album"]
        artists = album["artists"] + track["artists"]

        # album add
        if album["album_spotify_idx"] not in albums_dict:
            albums_dict[album["album_spotify_idx"]] = album

        # artists add
        for artist in artists:
            if artist["artist_spotify_idx"] not in artist_dict:
                artist_dict[artist["artist_spotify_idx"]] = artist
        print(album)
        print(artists[0])

    print(f"Tracks number after: {len(insert_lyrics_tracks)}")
    print("Exporting ...")
    with open(track_export_dir, "w", encoding="utf-8") as f:
        json.dump(insert_lyrics_tracks, f, ensure_ascii=False)
    with open(album_export_dir, "w", encoding="utf-8") as f:
        json.dump(list(albums_dict.values()), f, ensure_ascii=False)
    with open(artist_export_dir, "w", encoding="utf-8") as f:
        json.dump(list(artist_dict.values()), f, ensure_ascii=False)


if __name__ == "__main__":
    start_page, end_page = 0, 1  # TODO
    track_dir = (
        f"track_extra_dataset/filtered_data_{start_page:02d}_{end_page:02d}.json"
    )
    album_export_dir = (
        f"track_extra_dataset/album_data_{start_page:02d}_{end_page:02d}.json"
    )
    artist_export_dir = (
        f"track_extra_dataset/artist_data_{start_page:02d}_{end_page:02d}.json"
    )
    track_export_dir = (
        f"track_extra_dataset/track_data_{start_page:02d}_{end_page:02d}.json"
    )
    tracks_to_artists_albums(
        track_dir, album_export_dir, artist_export_dir, track_export_dir
    )
