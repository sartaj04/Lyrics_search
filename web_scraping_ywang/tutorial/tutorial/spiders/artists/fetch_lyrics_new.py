import csv
import re
import lyricsgenius as lg
import pandas
import requests
from bs4 import BeautifulSoup


access_token = 'dqGL_JaIaTxkHZsZq2gSy8_SvR8h5WhpEJ56hHLX2RolATalA5XLV4evQVnFm1hz'
genius = lg.Genius('dqGL_JaIaTxkHZsZq2gSy8_SvR8h5WhpEJ56hHLX2RolATalA5XLV4evQVnFm1hz', skip_non_songs=True, excluded_terms=["(Remix)", "(Live)"],
                             remove_section_headers=True)
artists = []

df = pandas.read_csv('artist_V.csv')

#df.iloc[start_index:end_index] to specify no of rows to read from dataframe 'df'
for index, row in df.iloc[0:2].iterrows():
    print(index, row['name'])
    artists.append(row['name'])


def get_lyrics(row):
    path = row['path']
    print(row['full_title'])
    url_prefix = 'https://genius.com'
    full_url = f'{url_prefix}{path}'
    #print(full_url)
    response_url = requests.get(full_url)
    soup = BeautifulSoup(response_url.text, 'html.parser')
    unformulated_lyrics = soup.find('div', {'class': 'Lyrics__Container-sc-1ynbvzw-6 YYrds'})
    try:
        if unformulated_lyrics is not None:
            lyrics_brackets = str(unformulated_lyrics.get_text(separator=' '))
            lyrics = re.sub(r'\[.*?\]', '', lyrics_brackets)
        else:
            lyrics = "Sorry, the lyrics of this song are not available."
    except (AttributeError, IndexError):
        lyrics = "Unexpected Error Occured!"
    return lyrics


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
                        header = songs_json[j].keys()
                        csv_writer.writerow(header)
                        count += 1
                    vals = songs_json[j].values()
                    title = songs_json[j]['title']
                    csv_writer.writerow(vals)


df_results = pandas.read_csv('Nithya_AVWXY.csv', encoding='utf-8')
df_results['lyrics'] = df_results.apply(lambda row: get_lyrics(row), axis=1)
df_results.to_csv('temp.csv', index=False)
