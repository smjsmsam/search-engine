import os
import csv
import json
import pandas as pd
from nltk.stem import PorterStemmer
import math

WEIGHT_IMPORTANT = 5
WEIGHT_STUFF = 1

def search_query(query: str):
    '''
    Main function to be called to search
    '''
    parsed = parse_query(query)    
    postings = get_postings(parsed)
    df = load_docids_csv()

    # Convert list-of-lists to list-of-dicts, for faster look up
    term_data_list = []
    for item in postings:
        # Convert list to dict: {docID: {freq_data}}
        posting_map = {p['document_id']: p['freq'] for p in item['postings']}
        doc_freq = len(posting_map)
        
        # Pre-calculate idf to improve speed
        idf = math.log10(len(df) / doc_freq) if doc_freq > 0 else 0

        term_data_list.append({'token': item['token'], 'map': posting_map, 'doc_freq': len(item['postings']), 'idf': idf})

    # Sort terms by document frequency (ascending)
    term_data_list.sort(key=lambda x: x['doc_freq'])

    # Boolean interection logic (using keys of the new maps)
    common_doc_ids = set(term_data_list[0]['map'].keys())
    for term_data in term_data_list[1:]:
        common_doc_ids.intersection_update(term_data['map'].keys())
        if not common_doc_ids: return []
    
    # Ranking (tf-idf)
    # Only ranking the documents that survived the intersection
    ranked_docids = []

    for docID in common_doc_ids:
        score = 0
        for term_data in term_data_list:
            # Find the specific posting for this docID
            freq_data = term_data['map'][docID]

            # Calculate tf
            # Wtd = 1 + log10(tf)
            raw_tf = (freq_data['important'] * WEIGHT_IMPORTANT) + (freq_data['stuff'] * WEIGHT_STUFF)
            tf = 1 + math.log10(raw_tf) if raw_tf > 0 else 0

            # Calculate idf
            # idf = log10(N/df)
            # doc_freq = term_data['doc_freq']
            # idf = math.log10(len(df) / doc_freq) if doc_freq > 0 else 0

            # idf pre-calculated above

            # Add to total score
            score += (tf * term_data['idf'])
        ranked_docids.append((docID, score))
    
    # Sort by score (descending)
    ranked_docids.sort(key=lambda x: x[1], reverse=True)

    # # --- DEBUG START ---
    # print("\n--- DEBUG: Top 10 Scores ---")
    # for i in range(min(10, len(ranked_docids))):
    #     doc_id, score = ranked_docids[i]
    #     print(f"Rank {i+1}: DocID {doc_id} | Score: {score:.4f} | URL: {getUrl(df, doc_id)}")
    # print("----------------------------\n")
    # # --- DEBUG END ---

    # Fetch URLs for Top 5
    top_5_urls = [getUrl(df, docid) for docid, score in ranked_docids[:5]]
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

    Returns postings
    [{'token': [str], 'postings': [{'document_id': [int], 'freq': {'important': [int], 'stuff': [int]}}]}, ...]
    '''
    postings = []
    
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
                    postings.append({"token": token, "postings": ""})
                    continue
                f.seek(pos)
                line = f.readline()
                token, posting = line.split(b':', 1)
                _postings = json.loads(posting.decode('utf-8'))
                postings.append({"token": token.decode('utf-8'), "postings": _postings})
        i += 1
    return postings
    

def load_docids_csv():
    '''
    Assumes that docids.csv is already sorted due to the nature of how it was
    created in index.py

    Returns a DataFrame for docids
    '''
    return pd.read_csv("docids.csv", dtype={'DOCID': 'Int64', 'URL': 'str'}, index_col="DOCID")


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