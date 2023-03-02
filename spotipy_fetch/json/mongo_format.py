import ast
import json
import pandas as pd
import os


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


def merge_artist(start_page=0, end_page=31):
    for idx in range(start_page, end_page + 1):
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
            json.dump(output, f, ensure_ascii=False, indent=4)


def merge_album(new_album_folder_dir="../album_dataset"):
    with open("artist_data.json", "r", encoding="utf-8") as f:
        artists = json.load(f)
    artists_dict = {artist["artist_spotify_idx"]: artist for artist in artists}
    untracked_artist_idxs = []
    handled_albums, unhandled_albums = [], []

    albums_files = find_files(new_album_folder_dir)
    album_dfs = [get_dataframe(album_dir) for album_dir in albums_files]
    # also load the unhandled albums:
    tb_updated_albums_df = pd.read_json("unhandled_albums.json")
    if not tb_updated_albums_df.empty:
        album_dfs.append(tb_updated_albums_df)
    concat_album_df = pd.concat(album_dfs)
    concat_album_df = concat_album_df.drop_duplicates()
    concat_album_df.rename(
        columns={"album_idx": "album_spotify_idx"}, inplace=True, errors="raise"
    )

    # convert into python dictionary
    albums = concat_album_df.to_dict("records")
    for a in albums:
        a["artists_idxs"] = ast.literal_eval(a["artists_idxs"])

        # if there is artists idx unrecorded, handle it later
        marked_tbc = all([artist_id in artists_dict for artist_id in a["artists_idxs"]])
        if marked_tbc:
            a["artists"] = [artists_dict[artist_id] for artist_id in a["artists_idxs"]]
            del a["artists_idxs"]
            handled_albums.append(a)
        else:
            untracked_artist_idxs.extend(a["artists_idxs"])
            unhandled_albums.append(a)

    untracked_artist_idxs = list(set(untracked_artist_idxs) - set(artists_dict.keys()))

    # load current history
    with open("album_data.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    output = merge_two_lists_dictionaries(data, handled_albums)
    with open("album_data.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=4)

    with open("unhandled_albums.json", "w", encoding="utf-8") as f:
        json.dump(unhandled_albums, f, ensure_ascii=False, indent=4)

    with open("untracked_artist_idxs.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    untracked_artist_idxs = list(set(data + untracked_artist_idxs))
    with open("untracked_artist_idxs.json", "w", encoding="utf-8") as f:
        json.dump(untracked_artist_idxs, f, ensure_ascii=False, indent=4)


def merge_track(new_track_folder_dir="../song_dataset"):
    with open("artist_data.json", "r", encoding="utf-8") as f:
        artists = json.load(f)
    artists_dict = {artist["artist_spotify_idx"]: artist for artist in artists}
    with open("album_data.json", "r", encoding="utf-8") as f:
        albums = json.load(f)
    albums_dict = {album["album_spotify_idx"]: album for album in albums}

    untracked_artist_idxs, untracked_album_idxs = [], []
    handled_tracks, unhandled_tracks = [], []

    tracks_file = find_files(new_track_folder_dir)
    track_dfs = [get_dataframe(album_dir) for album_dir in tracks_file]
    # also load the unhandled albums:
    tb_updated_tracks_df = pd.read_json("unhandled_tracks.json")
    if not tb_updated_tracks_df.empty:
        track_dfs.append(tb_updated_tracks_df)

    concat_track_df = pd.concat(track_dfs)
    concat_track_df = concat_track_df.drop_duplicates()
    tracks = concat_track_df.to_dict("records")

    for t in tracks:
        t["artists_spotify_idxs"] = ast.literal_eval(t["artists_spotify_idxs"])

        # if there is artists idx unrecorded, handle it later
        marked_tbc = all(
            [artist_id in artists_dict for artist_id in t["artists_spotify_idxs"]]
        )
        marked_tbc = marked_tbc and (t["album_spotify_idx"] in albums_dict)

        if marked_tbc:
            t["artists"] = [
                artists_dict[artist_id] for artist_id in t["artists_spotify_idxs"]
            ]
            t["album"] = albums_dict[t["album_spotify_idx"]]
            del t["artists_spotify_idxs"]
            del t["album_spotify_idx"]
            t["lyrics"] = None
            handled_tracks.append(t)
        else:
            untracked_artist_idxs.extend(t["artists_spotify_idxs"])
            untracked_album_idxs.append(t["album_spotify_idx"])
            unhandled_tracks.append(t)

    untracked_artist_idxs = list(set(untracked_artist_idxs) - set(artists_dict.keys()))
    untracked_album_idxs = list(set(untracked_album_idxs) - set(albums_dict.keys()))

    # load current history
    with open("track_data.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    output = merge_two_lists_dictionaries(data, handled_tracks)
    with open("track_data.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=4)

    with open("unhandled_albums.json", "w", encoding="utf-8") as f:
        json.dump(unhandled_tracks, f, ensure_ascii=False, indent=4)

    with open("untracked_artist_idxs.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    untracked_artist_idxs = list(set(data + untracked_artist_idxs))
    with open("untracked_artist_idxs.json", "w", encoding="utf-8") as f:
        json.dump(untracked_artist_idxs, f, ensure_ascii=False, indent=4)

    with open("untracked_album_idxs.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    untracked_album_idxs = list(set(data + untracked_album_idxs))
    with open("untracked_album_idxs.json", "w", encoding="utf-8") as f:
        json.dump(untracked_artist_idxs, f, ensure_ascii=False, indent=4)


merge_track("../new_track")