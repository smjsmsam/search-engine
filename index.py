import os
import json
from lxml import html, etree
from nltk.stem import PorterStemmer

DEV = True
PARTIAL_INDEX = {}
DOCID = 0

# inverted index: map(token, postings)
    # cannot hold all of index in memory
    # must offload hash map from main memory to
        # partial index on disk at least 3x during index construction,
        # and merge all partial indexes at the end
        # and optionally be split into seperate index files with term ranges

# posting: represent token's occurence in a document
    # must be variable-size
        # disk: continuous run of postings
        # memory: linked lists, variable-length arrays, associative arrays
    # document name/id found in
    # tf-idf score (only term frequency for M1)



# analytics:
    # number of indexed documents
    # number of unique tokens
    # total size (KB) of index on disk

# tokenizer -> text processing -> indexer

# indexer
    # from slides: term, doc, freq (terms and counts) -> posting lists (docIDs)
    # multiple term entries in single document are merged
    # split into dictionary and postings
    # add document frequency 

def initialize_index(data_path):
    global DOCID, PARTIAL_INDEX
    # PARTIAL_INDEX = {"letter": [{docID}: [frequency], . . .]}
    for domain, dirnames, filenames in os.walk(data_path):
        for file in filenames:
            DOCID += 1
            with open("docids.txt", 'a') as f:
                f.write(DOCID + '\n' + file + '\n')
            file_info = json.loads(os.path.join(domain, file))
            raw_text = file_info["content"]
            tokens = tokenize(raw_text)
            terms = process_tokens(tokens)
            postings = create_postings(terms)
            # append/create term's index based on posting, with docid
            # if index hash map is getting full, offload to partial index file
    # merge partial indexes (create separate files based on first letter of term?)


def tokenize(raw_text):
    '''
    remove html tags
    
    returns {"important": []], "stuff": []}
    '''
    try:
        tree = html.fromstring(raw_text)
    except Exception as e:
        print(e)
        return {}
    important_words = tree.xpath("//h1/text() | //h2/text() | //h3/text() | //strong/text() | //title/text()")
    etree.strip_elements(tree, 'script', 'style', 'template', 'meta', 'svg', 'embed', 'object', 'iframe', 'canvas', 'img', 'h1', 'h2', 'h3', 'strong', 'title')
    text_content = tree.text_content()
    words = text_content.split()
    return {"important": important_words, "stuff": words}


def process_tokens(tokens):
    '''
    tokens = {"important": [], "stuff": []}

    normalizes and stems tokens

    returns {"important": [], "stuff": []}
    '''
    
    # normalize
    # stemming
        # suggested: porter stemming
    
    # note: no stop words, keep them all
    pass

def create_postings(terms):
    

    global DOCID
    # associate with document id
    # tf-idf score (only term frequency for M1)
    total_weights = {}
    important_weights = frequencies(terms["important"], 10)
    stuff_weights = frequencies(terms["stuff"])
    for term in (important_weights, stuff_weights):
        for key, value in term.items():
            pass
    pass

def frequencies(items, weight=1):
    result = {}
    for item in items:
        if item not in result:
            result[item] = 0
        result[item] += weight
    return result

def main():
    global DEV
    data_path = "DEV" if DEV else "ANALYST"

    initialize_index(data_path)
