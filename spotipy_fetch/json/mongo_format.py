import ast
import json
import pandas as pd
import os
from pymongo import MongoClient
from bson.objectid import ObjectId


def get_dataframe(directory):
    df = pd.read_csv(directory, header=0)
    return df.drop_duplicates()


def find_files(search_path):
    result = [
        os.path.join(root, f) for root, _, files in os.walk(search_path) for f in files
    ]
    return result


def merge_two_lists_dictionaries(old, new):
    old_dump = [json.dumps(a) for a in old]
    new_dump = [json.dumps(a) for a in new]
    merged_dump = list(set(old_dump + new_dump))
    return [json.loads(x) for x in merged_dump]


class MongoDB:
    def __init__(self, client_dir="mongodb://localhost:27017/") -> None:
        my_client = MongoClient(client_dir)
        self.db = my_client["lyricsSearchEngine"]

    def search_mongo_spotify_idx(
        self, stf_idx, collection="tracks", get_duplicates=False
    ):
        mycol = self.db[collection]
        field_name = collection[:-1] + "_spotify_idx"
        res = list(mycol.find({field_name: stf_idx}))
        if len(res) == 0:
            return None
        doc = res[0]
        if len(res) > 1:
            print(f"ERROR: {stf_idx} in collection {collection} is duplicated.")
            if get_duplicates:
                return res
        return doc

    def clean_duplicates_mongo(self, collection="tracks"):
        mycol = self.db[collection]
        res = list(mycol.find({}))
        helper_dict = {}
        for doc in res:
            field_name = collection[:-1] + "_spotify_idx"
            stf_idx = doc[field_name]
            if stf_idx not in helper_dict:
                helper_dict[stf_idx] = []
            helper_dict[stf_idx].append(doc)

        # Keep the duplicates
        for key in list(helper_dict):
            if len(helper_dict[key]) == 1:
                del helper_dict[key]

        # If duplicates have the same artists name:
        # keep the first one (the popularity can be updated later)
        for key in list(helper_dict):
            value = helper_dict[key]
            field_name = collection[:-1] + "_name"
            all_equal = all(doc[field_name] == value[0][field_name] for doc in value)
            if all_equal:
                for doc in value[1:]:
                    try:
                        mycol.delete_one({"_id": ObjectId(doc["_id"])})
                    except Exception as e:
                        print(f"ERROR:{e}")
                del helper_dict[key]

        return helper_dict

    def insert_mongo(self, file_dir="track_data.json", collection="tracks"):
        mycol = self.db[collection]
        with open(file_dir, "r", encoding="utf-8") as f:
            data = json.load(f)
            mycol.insert_many(data)

    def update_mongo(self, collection="tracks"):
        pass


# Convert ObjectId into string
def object_id_to_str(obj):
    if "_id" in obj:
        obj["_id"] = str(obj["_id"])
    return obj


# Merge artists list based on Wasabi Dataset
def merge_artist(start_page=0, end_page=31):
    for idx in range(start_page, end_page):
        # load csv
        artist_df = get_dataframe(f"../artist_dataset/artist_id_spotipy{idx:02d}.csv")
        # rename column
        artist_df.rename(
            columns={"artist_id": "artist_spotify_idx"}, inplace=True, errors="raise"
        )
        # convert into python dictionary
        artists = list(artist_df.to_dict("index").values())
        for a in artists:
            a["artist_genres"] = ast.literal_eval(a["artist_genres"])

        # load current history
        with open("artist_data.json", "r", encoding="utf-8") as f:
            data = json.load(f)

        # set of two lists of dictionary
        output = merge_two_lists_dictionaries(data, artists)

        with open("artist_data.json", "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False)


def merge_album(new_album_folder_dir="../new_album"):
    db = MongoDB()
    artists = list(db.db["artists"].find({}))
    artists_dict = {artist["artist_spotify_idx"]: artist for artist in artists}

    untracked_artist_idxs = []
    handled_albums, unhandled_albums = [], []
    albums_files = find_files(new_album_folder_dir)
    album_dfs = [get_dataframe(album_dir) for album_dir in albums_files]
    albums = []

    if len(album_dfs) > 0:
        concat_album_df = pd.concat(album_dfs)
        concat_album_df = concat_album_df.drop_duplicates()
        concat_album_df.rename(
            columns={"album_idx": "album_spotify_idx"}, inplace=True, errors="raise"
        )
        albums = concat_album_df.to_dict("records")
        for a in albums:
            a["artists_idxs"] = ast.literal_eval(a["artists_idxs"])

    # also load the unhandled albums:
    with open("unhandled_albums.json", "r", encoding="utf-8") as f:
        tb_updated_albums = json.load(f)
    albums = merge_two_lists_dictionaries(albums, tb_updated_albums)

    for a in albums:
        # if there is artists idx unrecorded, handle it later
        artists_search = [
            object_id_to_str(artists_dict[artist_id])
            if artist_id in artists_dict
            else None
            for artist_id in a["artists_idxs"]
        ]
        marked_tbc = all(artists_search)

        if marked_tbc:
            a["artists"] = artists_search
            del a["artists_idxs"]
            handled_albums.append(a)
        else:
            for idx, artist in enumerate(artists_search):
                if artist == None:
                    untracked_artist_idxs.append(a["artists_idxs"][idx])
            unhandled_albums.append(a)

    untracked_artist_idxs = list(set(untracked_artist_idxs))

    # load current history
    with open("handled_albums.json", "w", encoding="utf-8") as f:
        json.dump(handled_albums, f, ensure_ascii=False)
    with open("unhandled_albums.json", "w", encoding="utf-8") as f:
        json.dump(unhandled_albums, f, ensure_ascii=False)

    with open("untracked_artist_idxs.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    untracked_artist_idxs = list(set(data + untracked_artist_idxs))
    with open("untracked_artist_idxs.json", "w", encoding="utf-8") as f:
        json.dump(untracked_artist_idxs, f, ensure_ascii=False)


def merge_track(new_track_folder_dir="../song_dataset"):
    db = MongoDB()
    artists = list(db.db["artists"].find({}))
    artists_dict = {artist["artist_spotify_idx"]: artist for artist in artists}
    albums = list(db.db["albums"].find({}))
    albums_dict = {album["album_spotify_idx"]: album for album in albums}

    untracked_artist_idxs, untracked_album_idxs = [], []
    handled_tracks, unhandled_tracks = [], []
    tracks_file = find_files(new_track_folder_dir)
    track_dfs = [get_dataframe(album_dir) for album_dir in tracks_file]
    tracks = []

    if len(track_dfs) > 0:
        concat_track_df = pd.concat(track_dfs)
        concat_track_df = concat_track_df.drop_duplicates()
        tracks = concat_track_df.to_dict("records")
        for t in tracks:
            t["artists_spotify_idxs"] = ast.literal_eval(t["artists_spotify_idxs"])

    # also load the unhandled albums:
    with open("unhandled_tracks.json", "r", encoding="utf-8") as f:
        tb_updated_tracks = json.load(f)
    tracks = merge_two_lists_dictionaries(tracks, tb_updated_tracks)

    for t in tracks:
        # if there is artists idx or album idx unrecorded, handle it later
        artists_search = [
            object_id_to_str(artists_dict[artist_id])
            if artist_id in artists_dict
            else None
            for artist_id in t["artists_spotify_idxs"]
        ]
        album_search = (
            object_id_to_str(albums_dict[t["album_spotify_idx"]])
            if t["album_spotify_idx"] in albums_dict
            else None
        )
        marked_tbc = all(artists_search) and album_search

        if marked_tbc:
            t["artists"] = artists_search
            t["album"] = album_search
            del t["artists_spotify_idxs"]
            del t["album_spotify_idx"]
            t["lyrics"] = None
            handled_tracks.append(t)
            pass
        else:
            for idx, artist in enumerate(artists_search):
                if artist == None:
                    untracked_artist_idxs.append(t["artists_spotify_idxs"][idx])
            if album_search == None:
                untracked_album_idxs.append(t["album_spotify_idx"])
            unhandled_tracks.append(t)

    untracked_artist_idxs = list(set(untracked_artist_idxs))
    untracked_album_idxs = list(set(untracked_album_idxs))

    # load current history
    with open("track_data.json", "w", encoding="utf-8") as f:
        json.dump(handled_tracks, f, ensure_ascii=False)
    with open("unhandled_tracks.json", "w", encoding="utf-8") as f:
        json.dump(unhandled_tracks, f, ensure_ascii=False)

    with open("untracked_artist_idxs.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    untracked_artist_idxs = list(set(data + untracked_artist_idxs))
    with open("untracked_artist_idxs.json", "w", encoding="utf-8") as f:
        json.dump(untracked_artist_idxs, f, ensure_ascii=False)

    with open("untracked_album_idxs.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    untracked_album_idxs = list(set(data + untracked_album_idxs))
    with open("untracked_album_idxs.json", "w", encoding="utf-8") as f:
        json.dump(untracked_album_idxs, f, ensure_ascii=False)


if __name__ == "__main__":
    merge_album("../new_album")
    # db = MongoDB()
    # db.insert_mongo("handled_albums.json", "albums")
    # db.clean_duplicates_mongo("albums")