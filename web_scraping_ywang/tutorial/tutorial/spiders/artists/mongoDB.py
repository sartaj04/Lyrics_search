import pandas as pd
from pymongo import MongoClient

client = MongoClient('mongodb://34.121.79.26:27017/')
db = client.Kaggle_Plus_1

#the csv file containing all the genius.py output
df = pd.read_csv('temp.csv')
data = df.to_dict('records')

collection = db.Kaggle_Plus_1
collection.insert_many(data)

client.close()
#Do not change any other info except file name in line 8 if needed. 
