#importing libraries
from bs4 import BeautifulSoup
import warnings
warnings.filterwarnings('ignore')
import itertools
import math
import re
import warnings
from os.path import exists
from stemming.porter2 import stem
import pandas as pd
# from search import englishST

warnings.filterwarnings('ignore')

stop = []
pos_index = {}
song_names = []


# process csv file
def csv_parser(path):
    file_map = {}
    data = pd.read_csv(path)
    song_names = data["title"].values
    Lyrics = data["lyrics"].values
    content_list = []
    # for i in range(len(song_names)):
    #     content = str(Lyrics[i])
    #     content = preprocess(content)
    #     content_list.append(content)
    #     file_map[song_names[i]] = content_list[i]
    file_map = dict(map(lambda i, j: (i, preprocess(j)), song_names, Lyrics))
    return file_map,song_names


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
def output_index(pi):
    filename = 'index.txt'
    with open(filename, 'w') as f:
        for key in sorted(pi):
            f.write(str(key) + ': ' + str(pi[key][0]) + '\n')
            for doc_no in pi[key][1]:
                word_pos = pi[key][1][doc_no]
                f.write('\t' + doc_no + ': ' + ','.join(str(pos+1) for pos in word_pos) + '\n')
            # f.write('\n')


# generate boolean query result file
def output_results_boolean(map_result):
    filename = 'result.boolean.txt'
    with open(filename, 'w') as f:
        for key in map_result:
            map_result[key][0] = sorted(list(map(int, map_result[key][0])))
            for i in map_result[key][0]:
                f.write(str(key) + ',' + str(i) + '\n')
        f.write('\n')


# generate ranked query result file
def output_results_ranked(map_result):
    filename = 'result.ranked.txt'
    with open(filename, 'w') as f:
        for key in map_result:
            for i in map_result[key][0]:
                f.write(str(key) + ',' + str(i) + '\n')
        f.write('\n')

# process query file
def read_queries(path):
    map_result = {}
    query_list = []
    query_no_list = []
    with open(path, 'r') as f_q:
        queries = f_q.read().splitlines()
        for line in queries:
            query_no = line[: line.index(' ')]
            query_no_list.append(query_no)
            query = line[line.index(' ') + 1:]
            query_list.append(query)
    # print(query_list)

    count = 0
    for query in query_list:
        result_list = []
        if 'AND' in query or 'OR' in query:
            part_result = boolean_search(query)
        elif '"' in query:
            part_result = phrase_search(query)
        elif '#' in query:
            part_result = proximity_search(query)
        elif ' ' not in query:
            part_result = word_search(query)
        else:
            part_result = tfidf(query)
        result_list.append(part_result)
        map_result[query_no_list[count]] = sorted(result_list)
        count = count + 1

    return map_result


# single word search
def word_search(query):
    neg = False
    if 'NOT' in query:
        query = query[4:]
        neg = True

    result = []
    term = query.strip('')
    term = preprocess(term)
    index = pos_index[term[0]]

    for doc_no in index[1]:
        result.append(doc_no)

    real_result = result
    if neg:
        real_result = negative(real_result)

    real_result = sorted(list(set(real_result)))
    #print("word_search")
    #print(real_result)
    return real_result


# phrase search
def phrase_search(query):
    neg = False
    if 'NOT' in query:
        query = query[4:]
        neg = True

    result = []
    terms = query.strip('"')
    term1, term2 = terms.split(' ', 1)
    term1 = preprocess(term1)
    term2 = preprocess(term2)
    index1 = pos_index[term1[0]]
    index2 = pos_index[term2[0]]

    for doc_no1 in index1[1]:
        for doc_no2 in index2[1]:
            if doc_no1 == doc_no2:
                for pos1 in index1[1][doc_no1]:
                    for pos2 in index2[1][doc_no2]:
                        if (pos2 - pos1) == 1:
                            result.append(doc_no1)
    real_result = result
    if neg:
        real_result = negative(real_result)

    real_result = sorted(list(set(real_result)))
    #print("phase_search")
    #print(real_result)
    return real_result


# proximity search
def proximity_search(query):
    neg = False
    if 'NOT' in query:
        query = query[4:]
        neg = True

    result = []
    distance = int(query[query.index('#') + 1: query.index('(')])
    term1 = query[query.index('('): query.index(',')].strip()
    term2 = query[query.index(','): query.index(')')].strip()
    term1 = preprocess(term1)
    term2 = preprocess(term2)
    index1 = pos_index[term1[0]]
    index2 = pos_index[term2[0]]

    for doc_no1 in index1[1]:
        for doc_no2 in index2[1]:
            if doc_no1 == doc_no2:
                for pos1 in index1[1][doc_no1]:
                    for pos2 in index2[1][doc_no2]:
                        if abs((pos2 - pos1)) <= distance:
                            result.append(doc_no1)
    real_result = result
    if neg:
        real_result = negative(real_result)

    real_result = sorted(list(set(real_result)))
    #print("proximity_search")
    #print(real_result)
    return real_result


# boolean search
def boolean_search(query):
    real_result = []
    if 'AND' in query:
        terms = query.split(' AND ')
        if '"' in terms[0]:
            result1 = phrase_search(str(terms[0]))
        elif '#' in terms[0]:
            result1 = proximity_search(str(terms[0]))
        else:
            result1 = word_search(str(terms[0]))

        if '"' in terms[1]:
            result2 = phrase_search(str(terms[1]))
        elif '#' in terms[1]:
            result2 = proximity_search(str(terms[1]))
        else:
            result2 = word_search(str(terms[1]))
        real_result = sorted(list(set(result1).intersection(set(result2))))

    if 'OR' in query:
        terms = query.split(' OR ')
        term1 = preprocess(terms[0])
        term2 = preprocess(terms[1])
        if '"' in term1:
            result1 = phrase_search(str(term1[0]))
        elif '#' in term1:
            result1 = proximity_search(str(term1[0]))
        else:
            result1 = word_search(str(term1[0]))

        if '"' in term2:
            result2 = phrase_search(str(term2[0]))
        elif '#' in term2:
            result2 = proximity_search(str(term2[0]))
        else:
            result2 = word_search(str(term2[0]))
        real_result = sorted(list(set(result1).union(set(result2))))

    return real_result


# deal with NOT
def negative(result):
    real_result = song_names
    for i in result:
        real_result.remove(str(i))
    return real_result


# ranked
def tfidf(query):
    terms = preprocess(query)
    score = {}
    # print(song_names)
    # print(pos_index)
    # print(terms)
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
        if i in range(0, 5):
            result_list.append(str(k) + ',' + ('%.4f' % v))

    return result_list


def query_search(query):
    global stop
    global pos_index
    global song_names
    stop = stopwords("/Users/sartajsyed/Downloads/django_project/search/englishST.txt")
    index_file = "/Users/sartajsyed/Downloads/django_project/search/index.txt"
    file_exists = exists(index_file)
    file_map,song_names = csv_parser("/Users/sartajsyed/Downloads/django_project/search/LyricsSmall.csv")
    if file_exists:  # checking if index file exists
        with open(index_file, "r") as file:
            content = file.read().splitlines()
        ii = {}
        inner_dict = {}
        lst = []
        for line in content:
            if '\t' not in line:
                inner_dict = {}
                line = line.replace(' ', "")
                line = line.split(':')
                term = line[0]
                freq = int(line[1])
                ii.setdefault(term, [])
                ii[term].append(int(freq))
                continue
            elif '\t' in line:
                line = line.replace('\t', "")
                line = line.replace(' ', "")
                line = line.split(':')
                lst1 = line[1]
                lst1 = lst1.split(",")
                # lst1 = [int(x) for x in lst1]
                inner_dict[line[0]] = lst1
                lst.append(inner_dict)
            if inner_dict not in ii[term]:
                ii[term].append(inner_dict)  # making inverted dictionary
        pos_index = ii
    else:
        ii = inverted_index(file_map)
        output_index(ii)
        pos_index = ii# if index file doesn't exist then initialising and creating it

    # output_results_ranked(read_queries("queries_lyrics.txt"))
    result = tfidf(query)
    return result

# if __name__ == "__main__":
#     main()
