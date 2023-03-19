# importing libraries
import warnings
warnings.filterwarnings('ignore')
import re
import warnings
from stemming.porter2 import stem
import pymongo
warnings.filterwarnings('ignore')
from pymongo import MongoClient

stop = []
pos_index = {}
track_index = []

clientlocal = MongoClient('mongodb://35.225.194.2:27017/')
client = MongoClient('mongodb://localhost:27017/')


def stopwords(path):
    global stop
    with open(path, 'r') as f_s:
        for x in f_s:
            stop.append(x.strip())

    return stop


# tokenization, remove stopwords, lower case, stemming
def preprocess(text):
    p_words = []
    tokenization = re.sub('\W', ' ', text.lower()).split()

    for word in tokenization:
        if word not in stop:
            if stem(word).strip() != "":
                p_words.append(stem(word).strip())
    return p_words


def generate_inverted_index(file_map):
    for key in file_map:
        wordlist = file_map[key]
        for pos, word in enumerate(wordlist):
            if word in pos_index:
                if key in pos_index[word][1]:
                    pos_index[word][1][key].append(pos)
                else:
                    pos_index[word][1][key] = [pos]
            else:
                pos_index[word] = []
                pos_index[word].append(1)
                pos_index[word].append({})
                pos_index[word][1][key] = [pos]

    for term in pos_index:
        for i in pos_index[term]:
            pos_index[term][0] = len(pos_index[term][1])

    return pos_index


def get_lyric_filemap():
    with client:
        db = client.trackInfo
        tracks = db.tracks.find().limit(100000)
        file_map = {}
        for track in tracks:
            if track['lyrics'] is not None:
                file_map[track['track_spotify_idx']] = preprocess(track['lyrics'])
                x = db.tracks.update_one({'_id': track['_id']},
                                     {'$set':
                                          {'lyric_filemap_length': len(preprocess(track['lyrics']))}
                                      })
    return file_map


def get_title_filemap():
    with client:
        db = client.trackInfo
        tracks = db.tracks.find().limit(100000)
        file_map = {}
        for track in tracks:
            file_map[track['track_spotify_idx']] = track['track_name']
            x = db.tracks.update_one({'_id': track['_id']},
                                     {'$set':
                                          {'title_filemap_length': len(preprocess(track['track_name']))}
                                      })
    return file_map


def get_artist_filemap():
    with client:
        db = client.trackInfo
        artists = db.artists.find().limit(100000)
        file_map = {}
        for artist in artists:
            file_map[artist['artist_spotify_idx']] = artist['artist_name']
            x = db.artists.update_one({'_id': artist['_id']},
                                     {'$set':
                                          {'artist_filemap_length': len(preprocess(artist['artist_name']))}
                                      })
    return file_map


def get_album_filemap():
    with client:
        db = client.trackInfo
        albums = db.albums.find().limit(100000)
        file_map = {}
        for album in albums:
            file_map[album['album_spotify_idx']] = album['album_name']
            x = db.albums.update_one({'_id': album['_id']},
                                     {'$set':
                                          {'album_filemap_length': len(preprocess(album['album_name']))}
                                      })
    return file_map


def output_index_into_mongodb(pi, column):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["indices"]
    if (column == "lyric"):
        mycol = mydb["lyricIndex"]
    elif(column == "title"):
        mycol = mydb["titleIndex"]
    elif(column == "artist"):
        mycol = mydb["artistIndex"]
    elif(column == "album"):
        mycol = mydb["albumIndex"]

    for key in sorted(pi):
        index_songs = []
        index_location = []
        for doc_no in pi[key][1]:
            word_pos = pi[key][1][doc_no]
            real_pos = []
            for pos in word_pos:
                real_pos.append(pos + 1)
            index_songs.append(doc_no)
            index_location.append(real_pos)
        mydict = {"index_name": str(key), "index_times": str(pi[key][0]), "index_songs": index_songs,
                  "index_location": index_location}
        x = mycol.insert_one(mydict)



def read_index_from_mongodb(column):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["indices"]
    if (column == "lyric"):
        mycol = mydb["lyricIndex"]
    elif (column == "title"):
        mycol = mydb["titleIndex"]
    elif (column == "artist"):
        mycol = mydb["artistIndex"]
    elif (column == "album"):
        mycol = mydb["albumIndex"]

    ii = {}
    for x in mycol.find():
        i = 0
        inner_dict = {}
        for song in x["index_songs"]:
            inner_dict[song] = x["index_location"][i]
            i = i + 1
        ii[x["index_name"]] = [x["index_times"], inner_dict]

    return ii


def read_related_info_from_mongodb(spotify_id, collections):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["trackInfo"]
    if collections == "lyric":
        mycol = mydb["track"]
        data = mycol.find_one({"track_spotify_idx": spotify_id})
        #track_info = [data['track_name'], data['artists'], data['album']]
    elif collections == "title":
        mycol = mydb["track"]
        data = mycol.find_one({"track_spotify_idx": spotify_id})
        #track_info = [data['track_name'], data['artists'], data['album']]
    elif collections == "artist":
        mycol = mydb["artist"]
        data = mycol.find_one({"artist_spotify_idx": spotify_id})
    elif collections == "album":
        mycol = mydb["album"]
        data = mycol.find_one({"album_spotify_idx": spotify_id})

    return data


if __name__ == "__main__":
    stop = stopwords("englishST.txt")

    lyricii = generate_inverted_index(get_lyric_filemap())
    output_index_into_mongodb(lyricii, "lyric")

    titleii = generate_inverted_index(get_title_filemap())
    output_index_into_mongodb(titleii, "title")

    artistii = generate_inverted_index(get_artist_filemap())
    output_index_into_mongodb(artistii, "artist")

    albumii = generate_inverted_index(get_album_filemap())
    output_index_into_mongodb(albumii, "album")

    #read_related_info_from_mongodb("3zaoqD3kMrSPW3l10XJumX","track")