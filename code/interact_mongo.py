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
myclient = pymongo.MongoClient("mongodb://localhost:27017/")

def stopwords(path):
    global stop
    with open(path, 'r') as f_s:
        for x in f_s:
            stop.append(x.strip())

    return stop

def preprocess_lyric(text):
    p_words = []
    tokenization = re.sub('\W', ' ', text.lower()).split()

    for word in tokenization:
        # if word not in stop:
        if stem(word).strip() != "":
            p_words.extend(stem(word).strip())
    return p_words


def preprocess_normal(text):
    p_words = []
    tokenization = re.sub('\W', ' ', text.lower()).split()

    for word in tokenization:
        if word not in stop:
            if stem(word).strip() != "":
                p_words.append(stem(word).strip())
    return p_words
# tokenization, remove stopwords, lower case, stemming

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




def get_lyric_filemap():
    db = myclient["trackInfo"]
    tracks = db.tracks.find()
    file_map = {}
    for track in tracks:
        if track['lyrics'] is not None:
            file_map[track['track_spotify_idx']] = preprocess_lyric(track['lyrics'])
            x = db.tracks.update_one({'_id': track['_id']},
                                 {'$set':
                                      {'lyric_filemap_length': len(preprocess_lyric(track['lyrics']))}
                                  })
    return file_map


def get_title_filemap():
    db = myclient["trackInfo"]
    tracks = db.tracks.find()
    file_map = {}
    for track in tracks:
        file_map[track['track_spotify_idx']] = track['track_name']
        x = db.tracks.update_one({'_id': track['_id']},
                                 {'$set':
                                      {'title_filemap_length': len(preprocess_normal(track['track_name']))}
                                  })
    return file_map


def get_artist_filemap():
    db = myclient["trackInfo"]

    artists = db.artists.find()
    file_map = {}
    for artist in artists:
        file_map[artist['artist_spotify_idx']] = artist['artist_name']
        x = db.artists.update_one({'_id': artist['_id']},
                                 {'$set':
                                      {'artist_filemap_length': len(preprocess_normal(artist['artist_name']))}
                                  })
    return file_map


def get_album_filemap():
    db = myclient["trackInfo"]

    albums = db.albums.find()
    file_map = {}
    for album in albums:
        file_map[album['album_spotify_idx']] = album['album_name']
        x = db.albums.update_one({'_id': album['_id']},
                                 {'$set':
                                      {'album_filemap_length': len(preprocess_normal(album['album_name']))}
                                  })
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
        index_ids = []
        index_location = []
        for doc_no in pi[key][1]:
            word_pos = pi[key][1][doc_no]
            real_pos = []
            for pos in word_pos:
                real_pos.append(pos + 1)
            index_ids.append(doc_no)
            index_location.append(real_pos)
        mydict = {"index_name": str(key), "index_times": str(pi[key][0]), "index_ids": index_ids,
                  "index_location": index_location}
        x = mycol.insert_one(mydict)


def read_filemap_from_db():
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["song"]
    mycol = mydb["details"]
    filemap = {}
    for x in mycol.find():
        filemap[x["song_name"]] = x["song_filemap_length"]

    return filemap

def output_updated_index_into_mongodb(pi):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["song"]
    mycol = mydb["index"]
    # myclient.drop_database('mycol')

    for key in sorted(pi):
        index_ids = []
        index_location = []
        for doc_no in pi[key][1]:
            word_pos = pi[key][1][doc_no]
            # real_pos = []
            # for pos in word_pos:
            #     # if pos not in real_pos:
            #     real_pos.append(pos + 1)
            index_ids.append(doc_no)
            index_location.append(list(set(word_pos)))

        query = {"index_name": str(key)}

        # create the update parameter with the fields to update
        update = {"index_times": str(pi[key][0]), "index_ids": index_ids, "index_location": index_location}

        # check if a document with the same index_name already exists in the collection
        result = mycol.find_one(query)

        if result is not None:
            # if a matching document exists, update it with the new values
            x = mycol.update_one(query, {"$set": update})
        else:
            # if no matching document exists, insert a new document
            mydict = {"index_name": str(key), "index_times": str(pi[key][0]), "index_ids": index_ids,
                      "index_location": index_location}
            x = mycol.insert_one(mydict)



def read_index_from_mongodb(search_type, query):
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
    real_query = []
    #?????????
    query = preprocess_normal(query)
    for term in query:
        myquery = {"index_name": term}
        x = mycol.find_one(myquery)
        i = 0
        inner_dict = {}
        if x is not None:
            for song in x["index_songs"]:
                inner_dict[song] = x["index_location"][i]
                i = i + 1
            ii[x["index_name"]] = [x["index_times"], inner_dict]
            real_query.append(x["index_name"])

    realquery_string = " ".join(real_query)

    return ii, realquery_string


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
    #read_index_from_mongodb("artist",preprocess_normal("Five Finger Death Punch"))
    #lyricii = generate_inverted_index(get_lyric_filemap())
    #output_index_into_mongodb(lyricii, "lyric")

    titleii = generate_inverted_index(get_title_filemap())
    output_index_into_mongodb(titleii, "title")

    artistii = generate_inverted_index(get_artist_filemap())
    output_index_into_mongodb(artistii, "artist")

    albumii = generate_inverted_index(get_album_filemap())
    output_index_into_mongodb(albumii, "album")

    #read_related_info_from_mongodb("3zaoqD3kMrSPW3l10XJumX","track")'''