import re

import requests
import csv
import pandas as pd
from bs4 import BeautifulSoup


def get_image(song_id):
    url = f"https://genius.com/songs/{song_id}"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    script_tag = soup.find('script', string=re.compile(r'window\.__PRELOADED_STATE__'))
    if script_tag:
        match = re.search(r'window\.__PRELOADED_STATE__', script_tag.string)
        if match:
            txt = match.string.strip().splitlines()[0].replace('\\', '')[40:-2]
            annot = txt.find('annotatable')
            temp = txt[annot:].split('}')[0]
            start = temp.find('imageUrl')
            end = temp.find('id')
            image_url = temp[start+11:end-3]
            if image_url:
                print(song_id, image_url)
                return image_url
            else:
                print(song_id, "No image found")
                return "No image found"


flag = False
#If your range starts from 100 - 500, then set skiprows = 99, nrows = 401
# You must already be having the ds2_ENGLISH.csv that we downloaded for running Histo's code. Use the same here.
df = pd.read_csv('C:\\Users\\Nithya\\Desktop\\ds2_ENGLISH\\ds2_ENGLISH.csv', skiprows=4, nrows=10, encoding='utf-8')
for index, row in df.iterrows():
    new_df = pd.DataFrame(row).T
    new_df['image'] = get_image(row[7])
    new_df.to_csv('kaggle_image.csv', mode='a', header=False, index=False)

#After execution of all your assigned range, please copy the below header to kaggle_image.csv file
#'title','tag','artist','year','views','features','lyrics','id','image'


