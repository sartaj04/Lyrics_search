# importing libraries
import warnings
warnings.filterwarnings('ignore')
import re
import json
import warnings
from stemming.porter2 import stem
import pymongo
warnings.filterwarnings('ignore')
from pymongo import MongoClient
from os.path import exists

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



def update_inverted_index(file_map):
    new_pos_index = {}
    for key in file_map:
        wordlist = file_map[key]
        for pos, word in enumerate(wordlist):
            if word in pos_index or word in new_pos_index:
                new_pos_index[word] = pos_index[word]
                if key in new_pos_index[word][1]:
                    new_pos_index[word][1][key].append(pos)
                else:

                    new_pos_index[word][1][key] = [pos]
            else:
                new_pos_index[word] = []
                new_pos_index[word].append(1)
                new_pos_index[word].append({})
                new_pos_index[word][1][key] = [pos]

    for term in new_pos_index:
        for i in new_pos_index[term]:
            new_pos_index[term][0] = len(pos_index[term][1])
    return new_pos_index




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
    with open('lyric_filemap.json', 'w') as fpl:
        json.dump(file_map, fpl)
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
    with open('title_filemap.json', 'w') as fpt:
        json.dump(file_map, fpt)

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
    with open('artist_filemap.json', 'w') as fpa:
        json.dump(file_map, fpa)
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
    with open('album_filemap.json', 'w') as fpb:
        json.dump(file_map, fpb)
    return file_map


def output_index_into_mongodb(pi, search_type):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["indices"]
    if (search_type == "lyric"):
        mycol = mydb["lyricIndex"]
    elif(search_type == "title"):
        mycol = mydb["titleIndex"]
    elif(search_type == "artist"):
        mycol = mydb["artistIndex"]
    elif(search_type == "album"):
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




def output_updated_index_into_mongodb(pi):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["song"]
    mycol = mydb["index"]

    for key in sorted(pi):
        index_songs = []
        index_location = []
        for doc_no in pi[key][1]:
            word_pos = pi[key][1][doc_no]
            index_songs.append(doc_no)
            index_location.append(list(set(word_pos)))

        query = {"index_name": str(key)}

        # create the update parameter with the fields to update
        update = {"index_times": str(pi[key][0]), "index_songs": index_songs, "index_location": index_location}

        # check if a document with the same index_name already exists in the collection
        result = mycol.find_one(query)

        if result is not None:
            # if a matching document exists, update it with the new values
            x = mycol.update_one(query, {"$set": update})
        else:
            # if no matching document exists, insert a new document
            mydict = {"index_name": str(key), "index_times": str(pi[key][0]), "index_songs": index_songs,
                      "index_location": index_location}
            x = mycol.insert_one(mydict)



def read_index_from_mongodb(search_type):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["indices"]
    if (search_type == "lyric"):
        mycol = mydb["lyricIndex"]
    elif (search_type == "title"):
        mycol = mydb["titleIndex"]
    elif (search_type == "artist"):
        mycol = mydb["artistIndex"]
    elif (search_type == "album"):
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


def read_related_info_from_mongodb(spotify_id, search_type):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["trackInfo"]
    mycol = mydb["track"]
    data = mycol.find_one({"track_spotify_idx": spotify_id})
    search_id = []
    if search_type == 'artist':
        for result in data['artists']:
            search_id.append(result['artist_spotify_idx'])
    elif search_type == 'album':
        for result in data['album']:
            search_id.append(result['album_spotify_idx'])
    elif search_type == 'song':
        search_id.append(spotify_id)

    return search_id



if __name__ == "__main__":
    stop = stopwords("englishST.txt")

    file1 = "lyric_filemap.json"
    file_exists1 = exists(file1)
    file2 = "title_filemap.json"
    file_exists2 = exists(file2)
    file3 = "album_filemap.json"
    file_exists3 = exists(file3)
    file4 = "artist_filemap.json"
    file_exists4 = exists(file4)
    if file_exists1:
        with open('lyric_filemap.json', 'r') as fp1:
            lyric_filemap = json.load(fp1)
    else:
        lyric_filemap =get_lyric_filemap()
    if file_exists2:
        with open('title_filemap.json', 'r') as fp2:
            title_filemap = json.load(fp2)
    else:
        title_filemap =get_title_filemap()
    if file_exists3:
        with open('album_filemap.json', 'r') as fp3:
            album_filemap = json.load(fp3)
    else:
        album_filemap =get_album_filemap()
    if file_exists4:
        with open('artist_filemap.json', 'r') as fp4:
            artist_filemap = json.load(fp4)
    else:
        artist_filemap =get_artist_filemap()


    lyricii = generate_inverted_index(lyric_filemap)
    with open('lyricii.json', 'w') as fp5:
        json.dump(lyricii, fp5)
    output_index_into_mongodb(lyricii, "lyric")

    titleii = generate_inverted_index(title_filemap)
    with open('titleii.json', 'w') as fp6:
        json.dump(titleii, fp6)
    output_index_into_mongodb(titleii, "title")

    artistii = generate_inverted_index(artist_filemap)
    with open('artistii.json', 'w') as fp7:
        json.dump(artistii, fp7)
    output_index_into_mongodb(artistii, "artist")

    albumii = generate_inverted_index(album_filemap)
    with open('albumii.json', 'w') as fp8:
        json.dump(albumii, fp8)
    output_index_into_mongodb(albumii, "album")

    #read_related_info_from_mongodb("3zaoqD3kMrSPW3l10XJumX","track")
