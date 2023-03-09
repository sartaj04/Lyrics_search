import csv
import re
import time
import lyricsgenius
import lyricsgenius as lg
import pandas
import requests
from bs4 import BeautifulSoup
from urllib3.exceptions import ReadTimeoutError

access_token = 'dqGL_JaIaTxkHZsZq2gSy8_SvR8h5WhpEJ56hHLX2RolATalA5XLV4evQVnFm1hz'
genius = lg.Genius('dqGL_JaIaTxkHZsZq2gSy8_SvR8h5WhpEJ56hHLX2RolATalA5XLV4evQVnFm1hz', skip_non_songs=True, excluded_terms=["(Remix)", "(Live)"],
                             remove_section_headers=True)
artists = []
df = pandas.read_csv('artist_V.csv')

#df.iloc[start_index:end_index] to specify no of rows to read from dataframe 'df'

for index, row in df.iloc[100:110].iterrows():
    print(index, row['name'])
    artists.append(row['name'])


def get_lyrics(song_id):
    url_prefix = 'https://genius.com/songs/'
    full_url = f'{url_prefix}{song_id}'
    print(full_url)
    try:
        response_url = requests.get(full_url)
        response_url.raise_for_status()
        soup = BeautifulSoup(response_url.text, 'html.parser')
        unformulated_lyrics = soup.find('div', {'class': 'Lyrics__Container-sc-1ynbvzw-6 YYrds'})
        if unformulated_lyrics is not None:
            lyrics_brackets = str(unformulated_lyrics.get_text(separator=' '))
            lyrics = re.sub(r'\[.*?\]', '', lyrics_brackets)
            return lyrics
        else:
            return "Sorry, the lyrics of this song are not available."
    except (AttributeError, IndexError):
        return "Unexpected Error Occured!"



def get_artist_songs(artistID):
    page_number = 1
    all_songs = []
    while True:
        songs_endpoint = f'https://api.genius.com/artists/{artistID}/songs?per_page=50&page={page_number}&access_token={access_token}'
        page_response = requests.get(songs_endpoint, headers=headers).json()
        songs = page_response['response']['songs']
        print("Page #:", page_number)
        if len(songs) == 0:
            break

        all_songs += songs
        page_number += 1

    print("Total songs: ", len(all_songs))
    return all_songs


count = 0
lyrics_list = []

'''
The below code directly appends the lyrics to each row and then it is written into the csv file. 
The approx. number of calls that can be used per day is around 10000. 
If the code breaks due to SSL Error, you can do the below:
    1. Copy paste the whole code into a new python file and run it again.
    2. Create a new genius token and re-run the program
However, this way the output file will consist of data until failure was observed, so you will 
know where to start from again. 
IMP: In case you decide to rerun the program, 
please copy the current output of the csv file you will use in below code without fail
'''

with open('Nithya_AVWXY.csv', mode='w', newline='', encoding='utf-8') as input_file:
    csv_writer = csv.writer(input_file)
    for artist in artists:
        if artist is None:
            print('Empty list')
        else:
            artists_endpoint = f'https://api.genius.com/search?q={artist}&access_token={access_token}'
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(artists_endpoint, headers=headers).json()
            artist_id = response['response']['hits'][0]['result']['primary_artist']['id']
            artist_name = response['response']['hits'][0]['result']['primary_artist']['name']
            print("Artist ID: ", artist_id)
            print("Artist Name: ", artist_name)
            songs_json = get_artist_songs(artist_id)
            for j in range(0, len(songs_json)):
                if (songs_json[j]['language'] != 'null') & (songs_json[j]['language'] == 'en'):
                    if count == 0:
                        header = list(songs_json[j].keys())
                        header.append('lyrics')
                        csv_writer.writerow(header)
                        count += 1
                    print(songs_json[j]['id'])
                    vals = list(songs_json[j].values())
                    res = get_lyrics(songs_json[j]['id'])
                    vals.append(res)
                    csv_writer.writerow(vals)
