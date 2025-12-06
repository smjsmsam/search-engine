import os
import csv
import orjson
import pandas as pd
from nltk.stem import PorterStemmer
import math
import atexit
import time
import bisect


WEIGHT_IMPORTANT = 5
WEIGHT_STUFF = 1
POSITION_WEIGHT = 0.3
PROXIMITY_BOOST = 1.2
EARLY_POSITION_BONUS = 0.5
CLUSTER_THRESHOLD = 5
CANDIDATE_THRESHOLD = 200
OFFSET_CACHE = {}
INDEX_FILE_POINTERS = {}
INDEX_INDEX_CSV_CACHE = {}
for letter in "0123456789abcdefghijklmnopqrstuvwxyz":
    path = f"indexes/{letter}.txt"
    csv_path = f"index/{letter}_index.csv"
    fp = open(path, "rb")
    INDEX_FILE_POINTERS[letter] = fp
    df = pd.read_csv(csv_path, dtype={'TOKEN': 'str', 'OFFSET': 'Int64'}, index_col="TOKEN")
    INDEX_INDEX_CSV_CACHE[letter] = list(df["OFFSET"].items())


DOCID_DF = pd.read_csv("docids.csv", dtype={'DOCID': 'Int64', 'URL': 'str'}, index_col="DOCID")
PS = PorterStemmer()


def search_query(query: str):
    '''
    Main function to be called to search
    '''
    parsed = parse_query(query)
    if not parsed: return []
    start = time.time()
    postings = get_postings(parsed)
    end = time.time()
    print(f"Getting postings took {end-start} seconds.")
    if not postings: return []
    start = time.time()
    ranked_docids = rank_postings(postings, DOCID_DF)
    end = time.time()
    print(f"Ranking took {end-start} seconds.")

    # # --- DEBUG START ---
    # print("\n--- DEBUG: Top 10 Scores ---")
    # for i in range(min(10, len(ranked_docids))):
    #     doc_id, score, tfidf, position = ranked_docids[i]
    #     print(f"Rank {i+1}: DocID {doc_id} | Total score: {score:.4f} | Tf-idf score: {tfidf:.4f} | Position score: {position:.4f} | URL: {getUrl(DOCID_DF, doc_id)}")
    # print("----------------------------\n")
    # # --- DEBUG END ---

    # Fetch URLs for Top 5
    top_5_urls = [getUrl(DOCID_DF, docid) for docid, score, tfidf, position in ranked_docids[:5]]
    return top_5_urls


def parse_query(query: str):
    '''
    Stems, strips, and sorts the query tokens
    '''
    return sorted(PS.stem(q.strip()) for q in query.split())


def get_postings(tokens: list[str]):
    '''
    Assumes that tokens is sorted

    Returns postings = {token: [{'document_id': [int], 'freq': {'important': [int], 'stuff': [int]}}]}
    '''
    postings = {}

    # get tokens that start with the same letter
    letters = {}
    for token in tokens:
        if token[0] in letters:
            letters[token[0]].append(token)
        else:
            letters[token[0]] = [token]
    
    for letter, tokens in letters.items():
        csv_path = f"index/{letter}_index.csv"
        # df = load_index_csv_cache(letter, csv_path)
        offsets = INDEX_INDEX_CSV_CACHE[letter]
        f = INDEX_FILE_POINTERS[letter]
        for token in tokens:
            pos = find_offset(INDEX_INDEX_CSV_CACHE[letter], token)
            if pos == None:
                continue
            f.seek(pos)
            line = f.readline()
            token, posting = line.split(b':', 1)
            start1 = time.time()
            _postings = orjson.loads(posting)
            end1 = time.time()
            print(f"loading took {end1-start1} seconds")
            postings[token] = _postings
    return postings


def rank_postings(postings, df):
    term_data_list = []

    # intersect documents
    for term, data in postings.items():
        # posting_map = {p['document_id']: p['freq'] for p in data}
        posting_map = {}
        for posting in data:
            posting_map[posting["document_id"]] = {"freq": posting["freq"],
                                                   "pos": posting.get("pos", [])}
        doc_freq = len(posting_map)
        if doc_freq == 0: continue

        # Pre-calculate idf to improve speed
        idf = math.log10(len(df) / doc_freq) if doc_freq > 0 else 0

        term_data_list.append({'token': term, 
                               'map': posting_map, 
                               'doc_freq': len(data), 
                               'idf': idf})

    # Sort terms by document frequency (ascending)
    term_data_list.sort(key=lambda x: x['doc_freq'])

    # Boolean intersection logic (using keys of the new maps)
    common_doc_ids = set(term_data_list[0]['map'].keys())
    for term_data in term_data_list[1:]:
        common_doc_ids.intersection_update(term_data['map'].keys())
        if not common_doc_ids: return []
    
    # Tf-idf
    # Only ranking the documents that survived the intersection
    tfidf_scores = {}

    for docID in common_doc_ids:
        tfidf_score = 0

        for term_data in term_data_list:
            # Find the specific posting for this docID
            freq_data = term_data["map"][docID]["freq"]

            # Calculate tf
            # Wtd = 1 + log10(tf)
            raw_tf = (freq_data['important'] * WEIGHT_IMPORTANT) + (freq_data['stuff'] * WEIGHT_STUFF)
            tf = 1 + math.log10(raw_tf) if raw_tf > 0 else 0
            tfidf_score += (tf * term_data['idf']) # idf pre-calculated above
        tfidf_scores[docID] = tfidf_score

    # proceed with the top few candidate documents
    candidate_ids = sorted(tfidf_scores.items(), key=lambda x: x[1], reverse=True)
    candidate_ids = [doc for doc, _ in candidate_ids[:CANDIDATE_THRESHOLD]]
    term_pairs = list(zip(term_data_list, term_data_list[1:]))
    ranked_docids = []

    for docID in candidate_ids:
        tfidf_score = tfidf_scores[docID]
        position_features = {"proximity_score": 0,
                             "early_position_bonus": 0,
                             "coherence_score": 0}
        all_positions = []

        for term_data in term_data_list:
            positions = term_data["map"][docID].get("pos", [])
            if positions:
                all_positions.extend(positions)
            if positions and len(positions) > 0:
                # apply bonus if term appears early
                earliest_pos = min(positions)
                if earliest_pos < 50:
                    position_features["early_position_bonus"] += EARLY_POSITION_BONUS * (1 - earliest_pos/50)
        
        proximity_scores = []

        for term1, term2 in term_pairs:
            positions1 = term1["map"][docID].get("pos", [])
            positions2 = term2["map"][docID].get("pos", [])
            if not positions1 or not positions2:
                continue
            
            min_distance = minimum_distance(positions1, positions2)
            score = 1 / (1 + min_distance / 10)
            proximity_scores.append(score)
        
        if proximity_scores:
            position_features['proximity_score'] = sum(proximity_scores) / len(proximity_scores)

        if all_positions:
            all_positions.sort()
            clusters = []
            current_cluster = [all_positions[0]]
            
            for pos in all_positions[1:]:
                if pos - current_cluster[-1] <= CLUSTER_THRESHOLD:
                    current_cluster.append(pos)
                else:
                    if len(current_cluster) >= 2:
                        clusters.append(current_cluster)
                    current_cluster = [pos]

            if len(current_cluster) >= 2:
                clusters.append(current_cluster)

            if clusters:
                total_density = 0
                for cluster in clusters:
                    # density = number of positions / span of cluster
                    span = cluster[-1] - cluster[0] + 1
                    density = len(cluster) / span
                    total_density += density
                position_features['coherence_score'] = total_density / len(clusters)
            else:
                position_features['coherence_score'] = 0
        
        # combine scores
        position_score = position_features["proximity_score"] * PROXIMITY_BOOST + \
                         position_features['early_position_bonus'] + \
                         position_features['coherence_score'] * 0.3
        
        final_score = tfidf_score + (position_score * POSITION_WEIGHT)
        ranked_docids.append((docID, final_score, tfidf_score, position_score))
    
    # Sort by score (descending)
    ranked_docids.sort(key=lambda x: x[1], reverse=True)
    return ranked_docids


def minimum_distance(a, b):
    i, j = 0, 0
    min = float("inf")

    while i < len(a) and j < len(b):
        x = a[i]
        y = b[j]
        if abs(x - y) < min:
            min = abs(x - y)
        if x < y:
            i += 1
        else:
            j += 1
    return min


def find_offset(offset_list, token):
    idx = bisect.bisect_left(offset_list, (token, -1))
    if idx < len(offset_list) and offset_list[idx][0] == token:
        return offset_list[idx][1]
    return None


def load_index_csv_cache(letter, path):
    if letter not in INDEX_INDEX_CSV_CACHE:
        INDEX_INDEX_CSV_CACHE[letter] = load_index_csv(path)
    return INDEX_INDEX_CSV_CACHE[letter]


def load_index_csv(filepath: str):
    '''
    Assumes that filepath is already sorted

    Returns a DataFrame for index offsets
    '''
    return pd.read_csv(filepath, dtype={'TOKEN': 'str', 'OFFSET': 'Int64'}, index_col="TOKEN")


def getUrl(df: pd.DataFrame, docid: int):
    '''
    Returns the docid url
    '''
    if docid in df.index:
        return df.loc[docid, 'URL']
    else:
        return None

@atexit.register
def last_report():
    for pointer in INDEX_FILE_POINTERS.values():
        pointer.close()
