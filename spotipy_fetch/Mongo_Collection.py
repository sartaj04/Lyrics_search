import json
import pandas as pd
from pymongo import MongoClient


class MongoCollection:
    def __init__(
        self,
        client_dir="mongodb://35.225.194.2:27017/autoReconnect=true&socketTimeoutMS=360000&connectTimeoutMS=360000",
        database="lyricsSearchEngine",
        collection="tracks",
    ) -> None:
        my_client = MongoClient(client_dir)
        db = my_client[database]
        self.col_name = collection
        self.col = db[collection]

    def search_mongo_spotify_idxs(self, stf_idxs, get_duplicates=False):
        field_name = self.col_name[:-1] + "_spotify_idx"

        def search_idx(stf_idx):
            res = list(self.col.find({field_name: stf_idx}))
            if len(res) == 0:
                return None
            doc = res[0]
            if len(res) > 1:
                print(f"ERROR: {stf_idx} in collection {self.col_name} is duplicated.")
                if get_duplicates:
                    return res
            return doc

        return [search_idx(idx) for idx in stf_idxs]

    def clean_duplicates_mongo(self, regex_exp):
        idx_field_name = self.col_name[:-1] + "_spotify_idx"
        db = MongoCollection(collection=self.col_name)
        res = list(
            db.col.find(
                {idx_field_name: {"$regex": regex_exp}}, {"_id": 1, idx_field_name: 1}
            )
        )
        df = pd.DataFrame(res)
        dedup = df.drop_duplicates(subset=idx_field_name, keep="first")
        remove = df[~df.apply(tuple, 1).isin(dedup.apply(tuple, 1))]
        if len(remove):
            remove_idxs = list(remove["_id"])
            threshold = 25000
            pages = -(-len(remove_idxs) // threshold)
            print(pages)
            for i in range(pages):
                print(i, end="starts ...")
                db.col.delete_many(
                    {"_id": {"$in": remove_idxs[i * threshold : (i + 1) * threshold]}}
                )
                print("end!")

    def insert_mongo(self, file_dir="track_data.json", page_range=None):
        sublist_len = 5000
        data_df = pd.read_json(file_dir)
        if page_range == None:
            max_page = -(-len(data_df) // sublist_len)
            print(f"Total pages : {max_page}")
            page_range = range(max_page)
        for idx in page_range:
            try:
                self.col.insert_many(
                    data_df[idx * sublist_len : (idx + 1) * sublist_len].to_dict(
                        "records"
                    )
                )
            except Exception as e:
                print(idx, e)