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
    rankedDocIDs = []
    for posting in postings:
        # TODO: selecting and ranking postings
        # posting looks like this
        # {'word': [word], 'postings': [{'document_id': [int], 'freq': {'important': [int], 'stuff': [int]}}]}
        pass
    top_5_urls = [getUrl(docID) for docID in rankedDocIDs[:5]]
    return top_5_urls


def parse_query(query: str, ps=PorterStemmer()):
    '''
    TODO: parse query words better -> importance, relevance, relative position, context, and stuff
    - Might need to change index.py for this
    - Currently stems, strips, and sorts the query words
    '''
    return sorted(ps.stem(q.strip()) for q in query.split())


def get_postings(words: list[str]):
    '''
    Assumes that words is sorted

    Returns postings
    [{'word': [word], 'postings': [{'document_id': [int], 'freq': {'important': [int], 'stuff': [int]}}]}, ...]
    '''
    postings = []
    
    i = 0
    words_length = len(words)
    while i < words_length:
        # get words that start with the same letter
        letter = words[i][0]
        word_list = [words[i]]
        while i+1 < words_length and words[i+1][0] == letter:
            i += 1
            word_list.append(words[i])
        csv_path = f"index/{letter}_index.csv"
        index_path = f"indexes/{letter}.txt"
        df = load_index_csv(csv_path)
        with open(index_path, 'rb') as f:
            for word in word_list:
                pos = get_offset(df, word)
                if pos == None:
                    postings.append({"word": word, "postings": ""})
                    continue
                f.seek(pos)
                line = f.readline()
                word, posting = line.split(b':', 1)
                _postings = json.loads(posting.decode('utf-8'))
                postings.append({"word": word.decode('utf-8'), "postings": _postings})
        i += 1
    

def create_index_of_index():
    '''
    Creates an index for each index in indexes
    '''
    for index in "0123456789abcdefghijklmnopqrstuvwxyz":
        words = []
        offsets = []
        csv_path = f"index/{index}_index.csv"
        index_path = f"indexes/{index}.txt"
        print(index)
        with open(index_path, 'rb') as f:
            offset = 0
            line = f.readline()
            while line:
                word, _ = line.split(b':', 1)
                words.append(word.decode('utf-8'))
                offsets.append(offset)
                offset = f.tell()
                line = f.readline()
        os.makedirs("index", exist_ok=True)
        with open(csv_path, 'w', newline='') as f:
            csvwriter = csv.writer(f)
            csvwriter.writerow(["WORD", "OFFSET"])
            for i in range(len(offsets)):
                csvwriter.writerow([words[i], offsets[i]])


def load_docids_csv():
    '''
    Assumes that docids.csv is already sorted due to the nature of how it was
    created in index.py

    Returns a DataFrame for docIds
    '''
    return pd.read_csv("docids.csv", dtype={'DOCID': 'Int64', 'URL': 'str'}, index_col="DOCID")


def load_index_csv(filepath: str):
    '''
    Assumes that filepath is already sorted

    Returns a DataFrame for index offsets
    '''
    return pd.read_csv(filepath, dtype={'WORD': 'str', 'OFFSET': 'Int64'}, index_col="WORD")


def get_offset(df: pd.DataFrame, word: str):
    '''
    Finds the offset of the word in the index
    '''
    if word in df.index:
        return df.loc[word, 'OFFSET']
    else:
        print("not here")
        return None


def getUrl(df: pd.DataFrame, docId: int):
    '''
    Returns the docId url
    '''
    if docId in df.index:
        return df.loc[docId, 'URL']
    else:
        return None