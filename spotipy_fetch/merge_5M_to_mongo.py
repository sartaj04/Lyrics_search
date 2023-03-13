import pandas as pd
from Mongo_Collection import MongoCollection
from utils import get_dataframe, find_files


track_dirs = find_files("../track_dataset/")


def get_tracks_df(page):
    dirs = [
        d
        for d in track_dirs
        if d.startswith(f"../track_dataset/track_id_spotipy{page:02d}")
    ]
    tdfs = [
        pd.read_csv(
            d,
            header=0,
            usecols=["artists_spotify_idxs", "track_spotify_idx", "track_name"],
        )
        for d in dirs
    ]
    return pd.concat(tdfs).drop_duplicates()


def update_lyrics(start_row=0, nrows=500000):
    df = pd.read_csv(
        "../../dataset/ds2.csv",
        usecols=["title", "artist", "lyrics"],
        nrows=nrows,
        skiprows=None if start_row == 0 else range(1, start_row),
        header=0,
    )

    df = df.sort_values(by=["artist"])
    df = df[df["lyrics"] != "[Instrumental]"]

    artist_dfs = [get_dataframe(d) for d in find_files("../artist_dataset/")]
    for page, adf in enumerate(artist_dfs):
        adf["page"] = page
    concat_adf = pd.concat(artist_dfs)
    concat_adf = concat_adf[["artist_name", "page", "artist_id"]].drop_duplicates()

    merged_df = df.merge(
        concat_adf, how="left", left_on="artist", right_on="artist_name"
    )
    search_df = merged_df[merged_df["artist_name"].notnull()]

    col = MongoCollection()
    for page in range(31):
        if not (page > 10 and page < 16):
            continue

        page_search_df = search_df[search_df["page"] == page]
        if page_search_df.empty:
            continue
        try:
            tdf = get_tracks_df(page)
            lyrics_found_df = page_search_df.merge(
                tdf, how="left", left_on="title", right_on="track_name"
            )
            if lyrics_found_df.empty:
                continue
            lyrics_found_df["correct_artists"] = lyrics_found_df.apply(
                lambda x: str(x.artist_id) in str(x.artists_spotify_idxs), axis=1
            )
            result_df = lyrics_found_df.loc[
                (lyrics_found_df["correct_artists"])
                & (lyrics_found_df["track_spotify_idx"].notnull())
            ]
            result_df = result_df.filter(items=["track_spotify_idx", "lyrics"])
            if not result_df.empty:
                for stf_idx, lyrics in result_df.values.tolist():
                    col.col.update_one(
                        {"track_spotify_idx": stf_idx},
                        {"$set": {"lyrics": lyrics}},
                        upsert=False,
                    )
        except Exception as e:
            print(page, e)


if __name__ == "__main__":
    update_lyrics(start_row=0, nrows=None)  # TODO
