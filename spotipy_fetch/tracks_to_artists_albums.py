import json
from Mongo_Collection import MongoCollection
from utils import object_id_to_str
import pandas as pd


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
        tracks_df = pd.read_json(f)

    # Search if exists in DB
    print(f"Tracks number before: {len(tracks_df)}")
    print("Search inserted data...")
    with open("tracks_intersection_stfidx.csv", "r", encoding="utf-8") as f:
        mongo_tracks_df = pd.read_csv(f)
    join_tracks_df = tracks_df.merge(
        mongo_tracks_df, on="track_spotify_idx", how="left", indicator=True
    )
    insert_lyrics_tracks = (
        join_tracks_df[join_tracks_df["_merge"] == "left_only"]
        .drop("_merge", 1)
        .to_dict("records")
    )

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
