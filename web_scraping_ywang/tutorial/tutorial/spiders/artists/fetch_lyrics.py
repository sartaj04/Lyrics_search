import csv
import time
import lyricsgenius
import lyricsgenius as lg
import pandas
import requests
from urllib3.exceptions import ReadTimeoutError

access_token = 'dqGL_JaIaTxkHZsZq2gSy8_SvR8h5WhpEJ56hHLX2RolATalA5XLV4evQVnFm1hz'
genius = lg.Genius('dqGL_JaIaTxkHZsZq2gSy8_SvR8h5WhpEJ56hHLX2RolATalA5XLV4evQVnFm1hz', skip_non_songs=True, excluded_terms=["(Remix)", "(Live)"],
                             remove_section_headers=True)
artists = []
df = pandas.read_csv('artist_V.csv')
#df.iloc[start_index:end_index] to specify no of rows to read from dataframe 'df'

for index, row in df.iloc[0:2].iterrows():
    print(index, row['name'])
    artists.append(row['name'])


def get_lyrics(song_name, artist_name):
    try:
        song = genius.search_song(song_name, artist_name)
        if song is None:
            return "Sorry, the lyrics of this song are not available."
        else:
            return song.lyrics
    except requests.exceptions.Timeout as e:
        print("Request timed out: {}".format(e))
        print("Retrying in 30 seconds...")
        time.sleep(30)
        return get_lyrics(song_name, artist_name)
    except lg.exceptions.Timeout as e:
        print("Timeout Error: {e}")
        print("Retrying in 30 secs...")
        time.sleep(30)
        return get_lyrics(song_name, artist_name)
    except requests.exceptions.ReadTimeout as e:
        print("Request timed out: {}".format(e))
        print("Retrying in 30 seconds...")
        time.sleep(30)
        return get_lyrics(song_name, artist_name)
    except ReadTimeoutError as e:
        print("Read timed out: {}".format(e))
        print("Retrying in 30 seconds...")
        time.sleep(30)
        return get_lyrics(song_name, artist_name)
    except lyricsgenius.exceptions.SongNotFound as e:
        # handle song not found exception
        print(f"Song not found error: {e}")
        return None


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
            print("Artist ID: ", artist_id)
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
                    res = get_lyrics(title, artist)
                    if res:  #
                        full_title = songs_json[j]['full_title']
                        lyrics_list.append(res.replace('\n', ' '))
                    else:
                        print('Could not get the lyrics')

df_results = pandas.read_csv('Nithya_AVWXY.csv', encoding='utf-8')
df_results['lyrics'] = lyrics_list
df_results.to_csv('Nithya_AVWXY.csv', index=False, mode='a', header=False)

print(df_results)