# importing libraries
import warnings
warnings.filterwarnings('ignore')
import math
import re
import warnings
from stemming.porter2 import stem
import pandas as pd
import pymongo
import numpy as np
from itertools import combinations
warnings.filterwarnings('ignore')

stop = []
#db
pos_index = {}
song_names = []
filemap = {}

#for test
def csv_parser(path):
    data = pd.read_csv(path)
    song_names = data["title"].values
    Lyrics = data["lyrics"].values
    file_map = dict(map(lambda i, j: (i, preprocess(j)), song_names, Lyrics))
    mongo_file_map = dict(map(lambda i, j: (i, j), song_names, Lyrics))

    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["song"]
    mycol = mydb["details"]

    for i in mongo_file_map:
        mydict = {"song_name": i, "song_lyrics": mongo_file_map[i], "song_filemap_length":len(preprocess(mongo_file_map[i]))}
        x = mycol.insert_one(mydict)

    return file_map, song_names



def output_index_into_mongodb(pi):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["song"]
    mycol = mydb["index"]

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
        #if word not in stop:
        if stem(word).strip() != "":
            p_words.append(stem(word).strip())
    return p_words


# generate inverted index
def inverted_index(file_map):
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


# generate index file
def output_index_into_txt(pi):
    filename = 'index.txt'
    with open(filename, 'w') as f:
        for key in sorted(pi):
            f.write(str(key) + ': ' + str(pi[key][0]) + '\n')
            for doc_no in pi[key][1]:
                word_pos = pi[key][1][doc_no]
                f.write('\t' + str(doc_no) + ': ' + ','.join(str(pos + 1) for pos in word_pos) + '\n')
            # f.write('\n')


def output_index_delta_encoding(pi):
    filename = 'index_delta1.txt'
    with open(filename, 'w') as f:
        for key in sorted(pi):
            f.write(str(key) + ': ' + str(pi[key][0]) + '\n')
            for doc_no in pi[key][1]:
                word_pos = pi[key][1][doc_no]
                f.write('\t' + str(doc_no) + ': ')
                last_pos = -1
                for v, pos in enumerate(word_pos):
                    if v == 0:
                        if len(word_pos) == 1:
                            f.write((str(pos + 1)))
                        else:
                            f.write((str(pos + 1)) + ',')
                        last_pos = pos + 1
                    elif v == len(word_pos) - 1:
                        f.write((str(pos + 1 - last_pos)))
                    else:
                        f.write((str(pos + 1 - last_pos)) + ',')
                        last_pos = pos + 1
                f.write('\n')


def output_index_into_mongodb(pi):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["song"]
    mycol = mydb["index"]

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


def read_from_mongodb():
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["song"]
    mycol = mydb["index"]

    ii = {}
    for x in mycol.find():
        i = 0
        inner_dict = {}
        for song in x["index_songs"]:
            inner_dict[song] = x["index_location"][i]
            i = i + 1
        ii[x["index_name"]] = [x["index_times"], inner_dict]

    return ii


def read_filemap_from_db():
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["song"]
    mycol = mydb["details"]
    filemap = {}
    for x in mycol.find():
        filemap[x["song_name"]] = x["song_filemap_length"]

    return filemap



def bm25(query):
    terms = preprocess(query)
    score = {}
    filemap = read_filemap_from_db()

    l=0
    for song1 in song_names:
        l+= filemap[song1]
    l_ = l/len(song_names)

    for song in song_names:
        weight = 0
        ld = filemap[song]
        k =1.5
        for term in terms:
            if song in pos_index[term][1]:
                dl = pos_index[term]
                tf_td = len(dl[1][song])
                dft = len(pos_index[term][1])
                # wtd1 = ((1 + math.log10(tf_td)) * math.log10(len(song_names) / dft))

                wtd2 = (tf_td/((k*(ld/l_))+tf_td+0.5))*math.log10((len(song_names)-dft+0.5)/(dft+0.5))
                weight = weight + wtd2
        score[str(song)] = weight

    score = sorted(score.items(), key=lambda x: -x[1])
    result_list = []

    for i, (k, v) in enumerate(score):
        if i in range(0, 10):
            result_list.append(str(k) + ',' + ('%.4f' % v))

    return result_list


def compute_tf(song_lyrics: list) -> dict:
    tf = {}
    for word in song_lyrics:
        tf[word] = tf.get(word, 0) + 1
    return tf


# ranked
def tfidf(query):
    terms = preprocess(query)
    score = {}
    for song in song_names:
        weight = 0
        for term in terms:
            if song in pos_index[term][1]:
                dl = pos_index[term]
                tf_td = len(dl[1][song])
                dft = len(pos_index[term][1])
                wtd = ((1 + math.log10(tf_td)) * math.log10(len(song_names) / dft))
                weight = weight + wtd
        score[str(song)] = weight

    score = sorted(score.items(), key=lambda x: -x[1])

    result_list = []

    for i, (k, v) in enumerate(score):
        if i in range(0, 10):
            result_list.append(str(k) + '|' + ('%.4f' % v))

    return result_list


def read_songs_from_db():
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["song"]
    mycol = mydb["details"]

    for x in mycol.find():
        song_names.append(x["song_name"])

    return song_names


def build_vocabulary(pos_index):
    return sorted(list(pos_index.keys()))


def build_tf_vector(query, vocabulary):
    tf_vector = [0] * len(vocabulary)

    for term in query:
        if term in vocabulary:
            index = vocabulary.index(term)
            tf_vector[index] += 1

    return tf_vector


def build_tfidf_vector(tf_vector, idf_vector):
    return [tf * idf for tf, idf in zip(tf_vector, idf_vector)]


def cosine_similarity(vec1, vec2):
    dot_product = np.dot(vec1, vec2)
    norm_vec1 = np.linalg.norm(vec1)
    norm_vec2 = np.linalg.norm(vec2)

    if norm_vec1 == 0 or norm_vec2 == 0:
        return 0

    similarity = dot_product / (norm_vec1 * norm_vec2)
    return similarity


def tfidf_cosine_similarity(query):
    query = preprocess(query)
    vocabulary = build_vocabulary(pos_index)

    # Calculate the IDF vector for the vocabulary
    idf_vector = []
    for term in vocabulary:
        idf = math.log(len(song_names) / int(pos_index[term][0]))
        idf_vector.append(idf)

    # Build the query TF vector and calculate the query TF-IDF vector
    query_tf_vector = build_tf_vector(query, vocabulary)
    query_tfidf_vector = build_tfidf_vector(query_tf_vector, idf_vector)

    # Initialize the similarities dictionary
    similarities = {}

    for song in song_names:
        # Build the song TF vector using the entire vocabulary
        song_terms = {}
        for term, (_, song_dict) in pos_index.items():
            if song in song_dict:
                song_terms[term] = song_dict[song]

        song_tf_vector = build_tf_vector(song_terms, vocabulary)
        song_tfidf_vector = build_tfidf_vector(song_tf_vector, idf_vector)

        # Calculate cosine similarity between query TF-IDF vector and song TF-IDF vector
        similarity = cosine_similarity(query_tfidf_vector, song_tfidf_vector)

        # Store the similarity in the similarities dictionary
        similarities[song] = similarity

    return similarities


def generate_permutations(tokens):
    num_tokens = len(tokens)
    permutations = []

    for length in range(num_tokens, 0, -1):
        for combination in combinations(tokens, length):
            permutations.append(' '.join(combination))

    # Remove duplicates and sort according to length
    sorted_permutations = sorted(list(set(permutations)), key=lambda x: (len(x), x), reverse=True)

    return sorted_permutations

def sort_similarities(similarities):
    sorted_similarities = sorted(similarities.items(), key=lambda x: x[1], reverse=True)
    result_list = []

    for i, (k, v) in enumerate(sorted_similarities):
        if i in range(0, 10):
            result_list.append(str(k) + ',' + ('%.4f' % v))

    return result_list


def phase_search(query):
    tokens = preprocess(query)
    num_tokens = len(tokens)
    if num_tokens == 1:
        # If the query consists of a single token, return the postings list for that token
        term = tokens[0]
        if term in pos_index:
            return list(pos_index[term][1])
        else:
            return []
    else:
        result = {}
        da = []
        permuta = generate_permutations(tokens)
        for a in permuta:
            da.append(preprocess(a))
        z = 0
        lengthresult = 0
        resultlist = {}
        for i in range(num_tokens):
            resultlist[i] = []

        while lengthresult < 20:
            # If the query consists of multiple tokens, perform phrase search
            positions = []
            for i, token in enumerate(da[z]):
                if i == 0:
                    # For the first token, initialize the positions with the postings list for the token
                    if token in pos_index:
                        positions = pos_index[token][1]
                    else:
                        return []
                else:
                    # For subsequent tokens, keep only the positions that are adjacent to the previous token
                    if token in pos_index:
                        new_pos = {}
                        for candidate in pos_index[token][1]:
                            if candidate in positions:
                                for pospos in positions[candidate]:
                                    for pos in pos_index[token][1][candidate]:
                                        distance = pos - pospos
                                        if distance == 1:
                                            new_pos[candidate] = pos_index[token][1][candidate]
                                            break
                        if i != len(tokens):
                            positions = new_pos
                            if len(positions) == 1:
                                result[z] = positions
                            if len(positions) == 0:
                                result[z] = {}
            for key in positions:
                resultlist[num_tokens - len(da[z])].append(key)
            resultlist[num_tokens - len(da[z])] = list(set(resultlist[num_tokens - len(da[z])]))
            lengthresult = 0
            for jj in range(len(resultlist)):
                lengthresult = len(resultlist[jj]) + lengthresult

            z = z + 1

    return resultlist


def lyric_search(query):

    tfidf_results = tfidf(query)
    phrase_search_results = phase_search(query)
    tfidf_scores = [result.split('|') for result in tfidf_results]
    tfidf_dict = {song: float(score) for song, score in tfidf_scores}

    phrase_scores = {}
    for num_tokens, song_list in phrase_search_results.items():
        for song in song_list:
            current_score = phrase_scores.get(song, 0)
            if num_tokens > current_score:
                phrase_scores[song] = len(preprocess(query)) - num_tokens

    normalized_tfidf_scores = normalize(tfidf_dict)
    normalized_phrase_scores = normalize(phrase_scores)
    weight_tfidf = 0.7
    weight_phrase = 0.3

    final_scores = {}
    for song in set(normalized_tfidf_scores.keys()) | set(normalized_phrase_scores.keys()):
        tfidf_score = normalized_tfidf_scores.get(song, 0)
        phrase_score = normalized_phrase_scores.get(song, 0)
        final_scores[song] = (weight_tfidf * tfidf_score) + (weight_phrase * phrase_score)

    ranked_songs = sorted(final_scores.items(), key=lambda x: x[1], reverse=True)

    top_songs = [song for song, score in ranked_songs[:20]]

    print(top_songs)

    return score



def tfidf_score_b(query):
    terms = preprocess_normal(query)
    score = {}
    for song in song_names:
        weight = 0
        for term in terms:
            if song in pos_index[term][1]:
                dl = pos_index[term]
                tf_td = len(dl[1][song])
                dft = len(pos_index[term][1])
                wtd = ((1 + math.log10(tf_td)) * math.log10(len(song_names) / dft))
                weight = weight + wtd
        score[str(song)] = weight

    return score



def normalize(scores):
    min_score = min(scores.values())
    max_score = max(scores.values())
    return {song: (score - min_score) / (max_score - min_score) for song, score in scores.items()}


def long_query_handling(query):
    query = preprocess(query)
    freq_list = []
    for i, word in enumerate(query):
        freq_list.append((word, pos_index[word], i))
    freq_list.sort(key=lambda x: (x[1], x[2]))
    top_10 = freq_list[:10]
    top_10.sort(key=lambda x: query.index(x[0]))
    to_string = lambda lst: ' '.join([t[0] for t in lst])
    top_10 = to_string(top_10)
    return top_10


def main():
    global stop
    global pos_index
    global song_names
    stop = stopwords("englishST.txt")

    song_names = read_songs_from_db()
    ii = read_from_mongodb()
    pos_index = ii
    '''
    file_map, song_names = csv_parser("LyricsSmall.csv")
    ii = inverted_index(file_map)
    output_index_into_txt(ii)
    output_index_into_mongodb(ii)
    pos_index = ii  # if index file doesn't exist then initialising and creating it
'''

    query = long_query_handling("You gotta Prada bag with a lotta stuff in it (uh, uh, uh) Give it to friend, let's spin Every lookin' at me, glancin' the kid")
    #similarities = tfidf_cosine_similarity(query)
    #sorted_similarities = sort_similarities(similarities)
    #print(sorted_similarities)
    tfidf_results = tfidf(query)
    #print(bm25(query))
    phrase_search_results = phase_search(query)

    tfidf_scores = [result.split('|') for result in tfidf_results]
    tfidf_dict = {song: float(score) for song, score in tfidf_scores}

    phrase_scores = {}
    for num_tokens, song_list in phrase_search_results.items():
        for song in song_list:
            current_score = phrase_scores.get(song, 0)
            if num_tokens > current_score:
                phrase_scores[song] = len(preprocess(query)) - num_tokens

    normalized_tfidf_scores = normalize(tfidf_dict)
    normalized_phrase_scores = normalize(phrase_scores)
    weight_tfidf = 0.7
    weight_phrase = 0.3

    final_scores = {}
    for song in set(normalized_tfidf_scores.keys()) | set(normalized_phrase_scores.keys()):
        tfidf_score = normalized_tfidf_scores.get(song, 0)
        phrase_score = normalized_phrase_scores.get(song, 0)
        final_scores[song] = (weight_tfidf * tfidf_score) + (weight_phrase * phrase_score)

    ranked_songs = sorted(final_scores.items(), key=lambda x: x[1], reverse=True)

    top_songs = [song for song, score in ranked_songs[:20]]

    print(top_songs)


if __name__ == "__main__":
    main()
