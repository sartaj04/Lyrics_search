import pandas as pd
from utils import create_spotipy, get_ymd


artist_limit = 250

# TODO
client_id = "0c525f920ffa4e6fbb3f538a4ece013f"
client_secret = "284dc2899d2844cbbc952e586c6959c5"


def get_albums(artist_page=0, album_page=0):
    df = pd.read_csv(f"./artist_dataset/artist_id_spotipy{artist_page:02d}.csv")
    artist_idxs = list(df["artist_id"])
    artist_idxs = sorted(list(set(artist_idxs)))
    partition_artist_idxs = [
        artist_idxs[i * artist_limit : (i + 1) * artist_limit]
        for i in range((len(artist_idxs) + artist_limit - 1) // artist_limit)
    ][album_page:]
    print("Total album page:", (len(artist_idxs) + artist_limit - 1) // artist_limit)

    stf = create_spotipy(client_id, client_secret)
    for count, partition in enumerate(partition_artist_idxs):
        print(count, "start... ", end="")
        album_results = []
        for artist_id in partition:
            uri = f"spotify:artist:{artist_id}"
            results = stf.artist_albums(uri, album_type="album")
            albums = results["items"]
            while results["next"]:
                results = stf.next(results)
                albums.extend(results["items"])

            for album in albums:
                year, month, day = get_ymd(album["release_date"])
                album_results.append(
                    {
                        "artists_idxs": [i["id"] for i in album["artists"]],
                        "album_idx": album["id"],
                        "album_name": album["name"],
                        "album_release_year": year,
                        "album_release_month": month,
                        "album_release_day": day,
                    }
                )
        print(count, "end!")

        pd.DataFrame(album_results).to_csv(
            f"./album_dataset/album_id_spotipy{artist_page:02d}_{count + album_page:02d}.csv",
            index=False,
        )


# TODO: specify the pages
get_albums(5)
