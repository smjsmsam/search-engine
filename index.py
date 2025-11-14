import os
import json
from lxml import html, etree
from nltk.stem import PorterStemmer
import re

DEV = True
PARTIAL_INDEX = {}
PARTIAL_LIST = []
DOCID = 0
PS = PorterStemmer()

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
            update_index(postings)
            # append/create term's index based on posting, with docid
            
                
        # offload to partial index file every ___ documents(?)

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
    
    important_words = tree.xpath("//h1/text() | //h2/text() | //h3/text()"
                                " | //strong/text() | //title/text()")
    
    etree.strip_elements(tree, 'script', 'style', 'template', 'meta',
                        'svg', 'embed', 'object', 'iframe', 'canvas',
                        'img', 'h1', 'h2', 'h3', 'strong', 'title')
    
    text_content = tree.text_content()
    words = text_content.split()
    return {"important": important_words, "stuff": words}



def process_tokens(tokens):
    '''
    tokens = {"important": [], "stuff": []}
    
    normalizes and stems tokens into terms
    '''
    return {"important": normalize_and_stem(tokens["important"]), \
            "stuff": normalize_and_stem(tokens["stuff"])}


def normalize_and_stem(tokens):
    '''
    reduces each token to alphanumeric lower case

    uses porter stemmer library to find stem word
    
    returns list of transformed tokens
    '''
    global PS
    result = []

    for token in tokens:
        norm = re.sub(r'[^a-zA-Z0-9]', '', token.lower())
        if norm:
            stemmed = PS.stem(norm)
            result.append(stemmed)
    return result


def create_postings(terms):
    '''
    terms = tokens = {"important": [], "stuff": []}

    returns {"[term]": {"document_id": [int],
     "freq": {"important": [int], "stuff": [int]}}} 
    '''
    global DOCID
    postings = {}

    important_weights = frequencies(terms["important"], 10)
    stuff_weights = frequencies(terms["stuff"])

    for term in stuff_weights.keys():
        postings[term] = {"document_id": DOCID, 
                          "freq": {"important": 0,
                                   "stuff": stuff_weights.get(term, 0)}}
        
    for term in important_weights.keys():
        if postings.get(term, None):
            postings[term]["freq"]["important"] = important_weights.get(term, 0)
        else:
            postings[term] = {"document_id": DOCID,
                              "freq": {"important": important_weights.get(term, 0),
                                       "stuff": 0}}
    # tf-idf score (only term frequency for M1) NGL IDK WHERE THiS GOES
    return sorted(postings.items(), key=lambda x: x[0])


def frequencies(items, weight=1):
    '''
    returns {"word": [weight]}
    '''
    result = {}

    for item in items:
        if item not in result:
            result[item] = 0
        result[item] += weight
    return result


def update_index(postings):
    # divide list into letters, already sorted by term name
    letters = {}
    for term, posting in postings.items():
        letters[term[0]][term] = posting
    i = 0
    
    # for each letter, add the new postings
    for letter, term in letters.items():
        index_path = "indexes/" + letter + ".txt"
        temp_path = index_path + ".tmp"
        
        if not os.path.exists(index_path):
            open(index_path, 'a').close()
        
        with open(index_path, 'r') as f, open(temp_path, 'w') as g:
            for line in f:
                current_term = line.split(':', 1)[0]

                while i < len(postings):
                    pass


def partial_offload():
    # check if need partial
    filepath = "partial" + str(len(PARTIAL_LIST)+1) + ".json"
    with open (filepath, "w") as f:
        # check json compatibility
        json.dump(PARTIAL_INDEX, f)
    PARTIAL_LIST.append(filepath)
    PARTIAL_INDEX = {}


def merge_partials():
    # for each partial in PARTIAL_LIST, merge contents to its respective index file(?)
    pass


def write_report():
    global DOCID
    #TODO: index = length of index keys after combining
    index = 0
    size = os.path.getsize("index.json") / 1024

    with open("report.txt", "w") as f:
        f.write(f"Indexed documents: {DOCID}\n")  # do we every skip
        f.write(f"Unique Tokens: {len(index)}\n")
        f.write(f"Total size: {size:.2f} KB\n")


def main():
    global DEV
    data_path = "DEV" if DEV else "ANALYST"

    initialize_index(data_path)
