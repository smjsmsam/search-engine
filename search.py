import os
import csv
import json
import pandas as pd
from nltk.stem import PorterStemmer
import math


WEIGHT_IMPORTANT = 5
WEIGHT_STUFF = 1
POSITION_WEIGHT = 0.3
PROXIMITY_BOOST = 1.2
EARLY_POSITION_BONUS = 0.5
CLUSTER_THRESHOLD = 50

DOCID_DF = pd.read_csv("docids.csv", dtype={'DOCID': 'Int64', 'URL': 'str'}, index_col="DOCID")


def search_query(query: str):
    '''
    Main function to be called to search
    '''
    parsed = parse_query(query)    
    postings = get_postings(parsed)
    ranked_docids = rank_postings(postings, DOCID_DF)

    # --- DEBUG START ---
    print("\n--- DEBUG: Top 10 Scores ---")
    for i in range(min(10, len(ranked_docids))):
        doc_id, score, tfidf, position = ranked_docids[i]
        print(f"Rank {i+1}: DocID {doc_id} | Total score: {score:.4f} | Tf-idf score: {tfidf:.4f} | Position score: {position:.4f} | URL: {getUrl(DOCID_DF, doc_id)}")
    print("----------------------------\n")
    # --- DEBUG END ---

    # Fetch URLs for Top 5
    top_5_urls = [getUrl(DOCID_DF, docid) for docid, score, tfidf, position in ranked_docids[:5]]
    return top_5_urls


def parse_query(query: str, ps=PorterStemmer()):
    '''
    TODO: parse query tokens better -> importance, relevance, relative position, context, and stuff
    - Might need to change index.py for this
    - Currently stems, strips, and sorts the query tokens
    *** whatever index does, it should be the same for parse_query()
    '''
    return sorted(ps.stem(q.strip()) for q in query.split())


def get_postings(tokens: list[str]):
    '''
    Assumes that tokens is sorted

    Returns postings = {token: [{'document_id': [int], 'freq': {'important': [int], 'stuff': [int]}}]}
    '''
    postings = {}
    
    i = 0
    tokens_length = len(tokens)
    while i < tokens_length:
        # get tokens that start with the same letter
        letter = tokens[i][0]
        token_list = [tokens[i]]
        while i+1 < tokens_length and tokens[i+1][0] == letter:
            i += 1
            token_list.append(tokens[i])
        csv_path = f"index/{letter}_index.csv"
        index_path = f"indexes/{letter}.txt"
        df = load_index_csv(csv_path)
        with open(index_path, 'rb') as f:
            for token in token_list:
                pos = get_offset(df, token)
                if pos == None:
                    # postings.append({"token": token, "postings": ""})
                    continue
                f.seek(pos)
                line = f.readline()
                token, posting = line.split(b':', 1)
                _postings = json.loads(posting.decode('utf-8'))
                # postings.append({"token": token.decode('utf-8'), "postings": _postings})
                postings[token] = _postings
        i += 1
    return postings


def rank_postings(postings, df):
    # Convert list-of-lists to list-of-dicts, for faster look up
    term_data_list = []
    for term, data in postings.items():
        # Convert list to dict: {docID: {freq_data}}
        # posting_map = {p['document_id']: p['freq'] for p in data}
        posting_map = {}
        for posting in data:
            posting_map[posting["document_id"]] = {"freq": posting["freq"],
                                                   "pos": posting.get("pos", [])}
        doc_freq = len(posting_map)
        
        # Pre-calculate idf to improve speed
        idf = math.log10(len(df) / doc_freq) if doc_freq > 0 else 0

        term_data_list.append({'token': term, 
                               'map': posting_map, 
                               'doc_freq': len(data), 
                               'idf': idf})

    # Sort terms by document frequency (ascending)
    term_data_list.sort(key=lambda x: x['doc_freq'])

    # Boolean interection logic (using keys of the new maps)
    common_doc_ids = set(term_data_list[0]['map'].keys())
    for term_data in term_data_list[1:]:
        common_doc_ids.intersection_update(term_data['map'].keys())
        if not common_doc_ids: return []
    
    # Ranking (tf-idf and position)
    # Only ranking the documents that survived the intersection
    ranked_docids = []

    for docID in common_doc_ids:
        tfidf_score = 0
        position_features = {"proximity_score": 0,
                             "early_position_bonus": 0,
                             "coherence_score": 0}
        all_term_positions = []

        for term_data in term_data_list:
            # Find the specific posting for this docID
            freq_data = term_data["map"][docID]["freq"]
            positions = term_data["map"][docID].get("pos", [])

            # Calculate tf
            # Wtd = 1 + log10(tf)
            raw_tf = (freq_data['important'] * WEIGHT_IMPORTANT) + (freq_data['stuff'] * WEIGHT_STUFF)
            tf = 1 + math.log10(raw_tf) if raw_tf > 0 else 0
            tfidf_score += (tf * term_data['idf']) # idf pre-calculated above

            if positions:
                all_term_positions.append((term_data["token"], positions))
            if positions and len(positions) > 0:
                # apply bonus if term appears early
                earliest_pos = min(positions)
                if earliest_pos < 50:
                    position_features["early_position_bonus"] += EARLY_POSITION_BONUS * (1 - earliest_pos/50)
        
        if len(all_term_positions) >= 2:
            all_distances = []
            all_positions = []

            # proximity score
            for i, (term1, positions1) in enumerate(all_term_positions):
                min_distances = []
                all_positions.extend(positions)
                for j, (term2, positions2) in enumerate(all_term_positions):
                    if i == j:
                        continue

                    # find minimum distance between any position of the two terms
                    min_distance = float("inf") # start with infinity
                    for pos1 in positions1:
                        for pos2 in positions2:
                            distance = abs(pos1 - pos2)
                            if distance < min_distance:
                                min_distance = distance
                    if min_distance != float("inf"):
                        min_distances.append(min_distance)
                if min_distances:
                    average_min = sum(min_distances) / len(min_distances)
                    score = 1 / (1 + average_min / 10)
                    all_distances.append(score)
            position_features["proximity_score"] = sum(all_distances) / len(all_distances) if all_distances else 0
            
            # coherence score
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
                total_cluster_score = 0
                for cluster in clusters:
                    # density = number of positions / span of cluster
                    span = cluster[-1] - cluster[0] + 1
                    density = len(cluster) / span
                    total_cluster_score += density
                position_features['coherence_score'] = total_cluster_score / len(clusters)
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


def load_index_csv(filepath: str):
    '''
    Assumes that filepath is already sorted

    Returns a DataFrame for index offsets
    '''
    return pd.read_csv(filepath, dtype={'TOKEN': 'str', 'OFFSET': 'Int64'}, index_col="TOKEN")


def get_offset(df: pd.DataFrame, token: str):
    '''
    Finds the offset of the token in the index
    '''
    if token in df.index:
        return df.loc[token, 'OFFSET']
    else:
        print("not here")
        return None


def getUrl(df: pd.DataFrame, docid: int):
    '''
    Returns the docid url
    '''
    if docid in df.index:
        return df.loc[docid, 'URL']
    else:
        return None
