import json
import pandas as pd
from pymongo import MongoClient


class MongoCollection:
    def __init__(
        self,
        client_dir="mongodb://localhost:27017/",
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

    def insert_mongo(self, file_dir="track_data.json"):
        with open(file_dir, "r", encoding="utf-8") as f:
            data = json.load(f)
            print(f"Insert {len(data)} data...")
            self.col.insert_many(data)

    def update_mongo(self):
        pass