import os
import csv
import json
import pandas as pd
from nltk.stem import PorterStemmer


def search_query(query: str):
    '''
    Main function to be called to search
    '''
    parsed = parse_query(query)    
    postings = get_postings(parsed)
    ranked_docids = []
    df = load_docids_csv()
    for posting in postings:
        # TODO: selecting and ranking postings
        # posting looks like this
        # {'token': [str], 'postings': [{'document_id': [int], 'freq': {'important': [int], 'stuff': [int]}}]}
        # boolean AND operation (optional), tf-idf scoring
        # *** we must use tf-idf
        if posting["postings"] != "":
            for p in posting["postings"]:
                # something
                ranked_docids.append(p['document_id'])
    top_5_urls = [getUrl(df, docid) for docid in ranked_docids[:5]]
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