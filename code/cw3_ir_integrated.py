# importing libraries
import math
import re
import warnings
from itertools import combinations

import numpy as np
from pymongo import MongoClient
from stemming.porter2 import stem
import itertools
import interact_mongo

warnings.filterwarnings('ignore')

clientlocal = MongoClient('mongodb://35.225.194.2:27017/')
client = MongoClient('mongodb://localhost:27017/')
stop = []
# db
pos_index = {}
spotify_ids = []
filemap = {}


def preprocess_lyric(text):
    p_words = []
    tokenization = re.sub('\W', ' ', text.lower()).split()

    for word in tokenization:
        # if word not in stop:
        if stem(word).strip() != "":
            p_words.append(stem(word).strip())
    return p_words


def preprocess_normal(text):
    p_words = []
    tokenization = re.sub('\W', ' ', text.lower()).split()

    for word in tokenization:
        if word not in stop:
            if stem(word).strip() != "":
                p_words.append(stem(word).strip())
    return p_words


def stopwords(path):
    global stop
    with open(path, 'r') as f_s:
        for x in f_s:
            stop.append(x.strip())

    return stop


def bm25(query,search_type):
    terms = preprocess_normal(query)
    score = {}
    filemap = interact_mongo.read_filemap_from_db(search_type,spotify_ids)

    l = 0
    for sid1 in spotify_ids:
        l += filemap[sid1]
    l_ = l / len(spotify_ids)

    for sid in spotify_ids:
        weight = 0
        ld = filemap[sid]
        k = 1.5
        for term in terms:
            if sid in pos_index[term][1]:
                dl = pos_index[term]
                tf_td = len(dl[1][sid])
                dft = len(pos_index[term][1])
                # wtd1 = ((1 + math.log10(tf_td)) * math.log10(len(song_names) / dft))

                wtd2 = (tf_td / ((k * (ld / l_)) + tf_td + 0.5)) * math.log10(
                    (len(spotify_ids) - dft + 0.5) / (dft + 0.5))
                weight = weight + wtd2
        score[str(sid)] = weight

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
def tfidf(query,spotify_ids,pos_index):
    terms = preprocess_lyric(query)
    score = {}
    for sid in spotify_ids:
        weight = 0
        for term in terms:
            if sid in pos_index[term][1]:
                dl = pos_index[term]
                tf_td = len(dl[1][sid])
                dft = len(pos_index[term][1])
                wtd = ((1 + math.log10(tf_td)) * math.log10(len(spotify_ids) / dft))
                weight = weight + wtd
        score[str(sid)] = weight

    score = sorted(score.items(), key=lambda x: -x[1])

    result_list = []

    for i, (k, v) in enumerate(score):
        if i in range(0, 10):
            result_list.append(str(k) + '|' + ('%.4f' % v))

    return result_list


#tfidf cosine
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
    query = preprocess_lyric(query)
    vocabulary = build_vocabulary(pos_index)

    # Calculate the IDF vector for the vocabulary
    idf_vector = []
    for term in vocabulary:
        idf = math.log(len(spotify_ids) / int(pos_index[term][0]))
        idf_vector.append(idf)

    # Build the query TF vector and calculate the query TF-IDF vector
    query_tf_vector = build_tf_vector(query, vocabulary)
    query_tfidf_vector = build_tfidf_vector(query_tf_vector, idf_vector)

    # Initialize the similarities dictionary
    similarities = {}

    for sid in spotify_ids:
        # Build the song TF vector using the entire vocabulary
        song_terms = {}
        for term, (_, song_dict) in pos_index.items():
            if sid in song_dict:
                song_terms[term] = song_dict[sid]

        song_tf_vector = build_tf_vector(song_terms, vocabulary)
        song_tfidf_vector = build_tfidf_vector(song_tf_vector, idf_vector)

        # Calculate cosine similarity between query TF-IDF vector and song TF-IDF vector
        similarity = cosine_similarity(query_tfidf_vector, song_tfidf_vector)

        # Store the similarity in the similarities dictionary
        similarities[sid] = similarity

    return similarities


def sort_similarities(similarities):
    sorted_similarities = sorted(similarities.items(), key=lambda x: x[1], reverse=True)
    return sorted_similarities


def phase_search(query, pos_index):
    tokens = preprocess_lyric(query)
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
            da.append(preprocess_lyric(a))
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


def generate_permutations(tokens):
    num_tokens = len(tokens)
    permutations = []

    for length in range(num_tokens, 0, -1):
        for combination in combinations(tokens, length):
            permutations.append(' '.join(combination))

    # Remove duplicates and sort according to length
    sorted_permutations = sorted(list(set(permutations)), key=lambda x: (len(x), x), reverse=True)

    return sorted_permutations



def lyric_search(query, spotify_id, pos_index):

    tfidf_results = tfidf(query,spotify_id, pos_index)
    phrase_search_results = phase_search(query, pos_index)
    tfidf_scores = [result.split('|') for result in tfidf_results]
    tfidf_dict = {song: float(score) for song, score in tfidf_scores}

    phrase_scores = {}
    for num_tokens, song_list in phrase_search_results.items():
        for song in song_list:
            current_score = phrase_scores.get(song, 0)
            if num_tokens >= current_score:
                phrase_scores[song] = len(preprocess_lyric(query)) - num_tokens

    normalized_tfidf_scores = normalize(tfidf_dict)
    normalized_phrase_scores = normalize(phrase_scores)
    weight_tfidf = 0.7
    weight_phrase = 0.3

    final_scores = {}
    for song in set(normalized_tfidf_scores.keys()) | set(normalized_phrase_scores.keys()):
        tfidf_score = normalized_tfidf_scores.get(song, 0)
        phrase_score = normalized_phrase_scores.get(song, 0)
        final_scores[song] = (weight_tfidf * tfidf_score) + (weight_phrase * phrase_score)

    ranked_songs = dict(sorted(final_scores.items(), key=lambda x: x[1], reverse=True))
    ranked_songs = dict(itertools.islice(ranked_songs.items(),20))

    #top_songs = [song for song, score in ranked_songs[:20]]

    print(ranked_songs)

    return ranked_songs


def normalize(scores):
    min_score = min(scores.values())
    max_score = max(scores.values())
    return {song: (score - min_score) / (max_score - min_score) for song, score in scores.items()}


def tfidf_ot(query,spotify_ids, pos_index):
    terms = preprocess_normal(query)
    score = {}
    for sid in spotify_ids:

        weight = 0
        for term in terms:
            if sid in pos_index[term][1]:
                dl = pos_index[term]
                tf_td = len(dl[1][sid])
                dft = len(pos_index[term][1])
                wtd = ((1 + math.log10(tf_td)) * math.log10(len(spotify_ids) / dft))
                weight = weight + wtd
        score[str(sid)] = weight

    score = sorted(score.items(), key=lambda x: -x[1])

    result_list = {}

    for i, (k, v) in enumerate(score):
        if i in range(0, 20):
            result_list[str(k)] = ('%.4f' % v)

    return result_list


def combine_search(query_a, query_b, search_type, search_a = lyric_search, search_b = tfidf_ot, num_top_search = 20,
                   coefficient_a = .7, coefficient_b =.3):
    result_list = []
    pos_index_a = {}
    spotify_ids_a = []
    pos_index_b = {}
    spotify_ids_b = []
    if query_a == "":
        if len(query_b.split()) > 10:
            query_b = long_query_handling(query_b)
        spotify_ids_b = interact_mongo.read_filemap_key_from_json(search_type)
        pos_index_b, real_query_b = interact_mongo.read_index_from_json(search_type, query_b)
        # it will return album , artist name or song (by song title)
        # case: only search b
        score_total = sorted(search_b(real_query_b, spotify_ids_b, pos_index_b).items(), key=lambda x: -x[1])
    else:
        if len(query_a.split()) > 10:
            query_a = long_query_handling(query_a)
        spotify_ids_a = interact_mongo.read_filemap_key_from_json('lyric')
        pos_index_a, real_query_a = interact_mongo.read_index_from_json('lyric', query_a)
        score_a = search_a(real_query_a, spotify_ids_a,pos_index_a)
        if query_b == "":
            # case: only search a
            score_total = sorted(score_a.items(), key=lambda x: -x[1])
        else:
            # combine search on search a and search b
            if len(query_b.split()) > 10:
                query_b = long_query_handling(query_b)
            spotify_ids_b = interact_mongo.read_filemap_key_from_json(search_type)
            pos_index_b, real_query_b = interact_mongo.read_index_from_json(search_type, query_b)
            score_b = search_b(real_query_b, spotify_ids_b, pos_index_b)
            score_total = {}
            for (k, v) in enumerate(score_a):
                # if i in range(0, num_top_search):
                # k is the song id
                # using k to search in DB for score b id and its song name
                id_b_list = interact_mongo.read_related_info_from_mongodb(v, search_type)
                max_score_b = 0
                for id_b in id_b_list:
                    if id_b in score_b.keys():
                        if float(score_b[id_b]) > max_score_b:
                            max_score_b = float(score_b[id_b])
                # only get the max score from search b
                # v is the score a from that song; k is the song id
                score_total[v] = coefficient_a * score_a[v] + coefficient_b * max_score_b
            score_total = sorted(score_total.items(), key=lambda x: -x[1])

    # k can be the song, album, artist id and v is their corresponding score
    for i, (k, v) in enumerate(score_total):
        if i in range(0, 10):
            result_list.append(str(k) + ',' + ('%.4f' % v))

    return result_list


def long_query_handling(query):
    query = preprocess_normal(query)
    freq_list = []
    for i, word in enumerate(query):
        freq_list.append((word, pos_index[word], i))
    freq_list.sort(key=lambda x: (x[1], x[2]))
    top_10 = freq_list[:10]
    top_10.sort(key=lambda x: query.index(x[0]))
    to_string = lambda lst: ' '.join([t[0] for t in lst])
    top_10 = to_string(top_10)
    return top_10


'''
def ir_from_index(query_a, query_b, search_type):
    global stop
    global pos_index
    global spotify_ids
    stop = stopwords("englishST.txt")

    if query_a == "":
        if len(query_b.split()) > 10:
            query_b = long_query_handling(query_b)
        spotify_ids = interact_mongo.read_filemap_key_from_json(search_type)
        pos_index, real_query = interact_mongo.read_index_from_json(search_type, query_b)
        score_b = tfidf_ot(query_b)
        print(score_b)
        #cs
    else:
        if len(query_a.split()) > 10:
            query_a = long_query_handling(query_a)
        spotify_ids = interact_mongo.read_filemap_key_from_json('lyric')
        pos_index, real_query = interact_mongo.read_index_from_json('lyric', query_a)
        resulta = lyric_search(query_a,spotify_ids,pos_index)
        print(resulta)
        # cs
        if query_b == "":
            print()
        else:
            if search_type != "":
                if len(query_b.split()) > 10:
                    query_b = long_query_handling(query_b)
                spotify_ids = interact_mongo.read_filemap_key_from_json(search_type)
                pos_index, real_query = interact_mongo.read_index_from_json(search_type,query_b)
                combine_search(query_a, query_b, search_type)
                #score_b = search_b(query_b)
'''


print(combine_search("love you","love you","album"))