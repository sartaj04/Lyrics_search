import os
import json
import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

# Current your account to fetch data. These info should be private.
def create_spotipy(client_id="", client_secret=""):
    client_credentials_manager = SpotifyClientCredentials(
        client_id=client_id, client_secret=client_secret
    )
    stf = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    return stf


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


# Convert ObjectId into string
def object_id_to_str(obj):
    if "_id" in obj:
        obj["_id"] = str(obj["_id"])
    return obj