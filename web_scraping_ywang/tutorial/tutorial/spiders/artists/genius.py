import csv
import re
import pandas as pd
import requests
from bs4 import BeautifulSoup
from langdetect import detect, detect_langs
import json


def get_lyrics(sent):
    l = ''
    for each in sent:
        a = str(each).replace('<br/>', '\\n')
        soup = BeautifulSoup(a, 'html.parser')
        divtags = soup.find_all('div')
        for div in divtags:
            cont = div.get_text()
            l += str(cont)
            #print(str(l))
    if len(l) >10:
        return l
    else:
        return 'This song has no lyrics'


def get_image(soup_1):
    #print('In Get Image')
    script_tag = soup_1.find('script', string=re.compile(r'window\.__PRELOADED_STATE__'))
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
                return image_url
            else:
                return "No image found"


def get_details(soup_1):
    #print(soup_1)
    targeting_list_match = re.search(r"var targeting_list\s*=\s*(.*?);", str(soup_1), re.DOTALL)
    #print(targeting_list_match)
    det_dict = {}
    if targeting_list_match:
        targeting_list_str = targeting_list_match.group(1)
        #print(targeting_list_str)
        json_obj = json.loads(targeting_list_str)
        #print(json_obj)
        for obj in json_obj:
            name = obj['name']
            values = obj['values']
            #print(f"{name}: {values}")
            det_dict[name] = values
    #print(det_dict)
    return det_dict


with open('temp2.csv', mode='w', newline='', encoding='utf-8') as input_file:
    csv_writer2 = csv.writer(input_file)
    header = ['title', 'tag', 'artist', 'year', 'image', 'views', 'lyrics', 'id']
    csv_writer2.writerow(header)
    #change the range below, preferably with a diff of 500
    for id in range(8042871, 8042890):
        print(id)
        url = f"https://genius.com/songs/{id}"
        response = requests.get(url)
        soup1 = BeautifulSoup(response.text, 'html.parser')
        soup2 = BeautifulSoup(response.content.decode('utf-8'), 'html.parser')
        send = soup2.find_all('div', class_='Lyrics__Container-sc-1ynbvzw-6 YYrds')
        final_lyrics = get_lyrics(send)
        image = get_image(soup1)
        details = get_details(soup1)
        #print(details)
        if final_lyrics != 'This song has no lyrics' and int(details['pageviews'][0]) > 50 and detect(final_lyrics) == 'en':
            result = [details['song_title'][0], details['primary_tag'][0], details['artist_name'][0], details['release_year'][0], image, details['pageviews'][0], str(final_lyrics), details['song_id'][0]]
            csv_writer2.writerow(str(elem) for elem in result)
        else:
            print("No lyrics for instrumentals.")


cols = ['title', 'tag', 'artist', 'year', 'image', 'views', 'lyrics', 'id']
append_data = pd.read_csv('temp_3.csv', header=None, names=cols, skiprows=1)
current_data = pd.read_csv('temp2.csv', delimiter=',', skip_blank_lines=True)
updated_data = append_data.append(current_data, ignore_index=True)
#This csv file to be used as it will contain all data after multiple runs
updated_data.to_csv('temp_3.csv', index=False)
