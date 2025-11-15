import os
import shutil
import json
import re
from lxml import html, etree
from nltk.stem import PorterStemmer
from collections import defaultdict

DEV = True
PARTIAL_INDEX = []
PARTIAL_LIST = []
DOCID = 0
PS = PorterStemmer()
POSTING_COUNT = 0
POSTING_THRESHOLD = 5000

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
    global DOCID, POSTING_COUNT, PARTIAL_INDEX
    for domain, dirnames, filenames in os.walk(data_path):
        for file in filenames:
            file_info = {}
            file_path = os.path.join(domain, file)
            print("Indexing " + file_path)

            with open(file_path, 'r') as f:
                file_info = json.load(f)
            raw_text = file_info["content"]
            
            # skip empty content
            if not raw_text:
                continue
            
            DOCID += 1
            with open("docids.txt", 'a') as f:
                f.write(str(DOCID) + '\n' + file_info["url"] + '\n')

            tokens = tokenize(raw_text)
            # print("Tokens: " + str(tokens))
            terms = process_tokens(tokens)
            # print("Terms: " + str(terms))
            postings = create_postings(terms)
            # print("Postings: " + str(postings))
            PARTIAL_INDEX.extend(postings.items())
            POSTING_COUNT += len(postings)
            # offload partial index (postings)
            if POSTING_COUNT >= POSTING_THRESHOLD:
                update_index()
    if PARTIAL_INDEX:
        update_index()
    write_report()


def tokenize(raw_text):
    '''
    remove html tags
    
    returns {"important": []], "stuff": []}
    '''
    try:
        tree = html.fromstring(raw_text.encode())
    except Exception as e:
        print(e)
        return {"important": [], "stuff": []}
    
    important_words = []
    important_text = tree.xpath("//h1/text() | //h2/text() | //h3/text()"
                                " | //strong/text() | //title/text()")
    for text in important_text:
        important_words.extend(text.split())
    
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
        if token != []:
            norm = re.sub(r'[^a-zA-Z0-9]', '', token.lower())
            if norm:
                stemmed = PS.stem(norm)
                result.append(stemmed)
    return result


def create_postings(terms):
    '''
    terms = tokens = {"important": [], "stuff": []}

    returns [{"[term]": {"document_id": [int],
     "freq": {"important": [int], "stuff": [int]}}}]
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
    return postings


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


def update_index():
    '''
    PARTIAL_INDEX = [{"[term]": {"document_id": [int],
     "freq": {"important": [int], "stuff": [int]}}}]
    '''
    global PARTIAL_INDEX
    # divide list into letters, and sort terms alphabetically
    # letters = {"a": {"apple": [posting1, posting2, ...]}}
    sorted_postings = sorted(PARTIAL_INDEX, key=lambda x: x[0])
    letters = {}
    for term, posting in sorted_postings:
        if term[0] not in letters:
            letters[term[0]] = {}
            letters[term[0]][term] = [posting]
        else:
            if term not in letters[term[0]]:
                letters[term[0]][term] = [posting]
            else:
                letters[term[0]][term].extend(posting)
    
    #make indexes folder if it doesn't exist
    os.makedirs("indexes", exist_ok=True)

    # for each letter, add the new postings
    for letter, terms_dict in letters.items():
        terms = list(terms_dict.items())
        i = 0
        total_new = len(terms)

        index_path = "indexes/" + letter + ".txt"
        temp_path = index_path + ".tmp"

        # if file does not exist, make an empty file
        if not os.path.exists(index_path):
            open(index_path, 'w').close()

        # insert into temporary copy
        with open(index_path, 'r') as f, open(temp_path, 'w') as g:
            # file format = term:{posting}, {posting}, ...
            for line in f:
                current_term, current_list = line.split(':', 1)

                # insert new term into alphabetically sorted spot
                while i < total_new and terms[i][0] < current_term:
                    term, posting = terms[i]
                    g.write(term + ":" + json.dumps(posting) + "\n")
                    i += 1

                # append to existing term
                if i < total_new and terms[i][0] == current_term:
                    term, posting = terms[i]
                    try:
                        current_postings = json.loads(current_list)
                    except json.JSONDecodeError:
                        current_postings = []
                    # appended = line.rstrip('\n') + ", " + json.dumps(posting) + "\n"
                    merged = current_postings + terms[i][1]
                    g.write(term + ":" + json.dumps(merged) + "\n")
                    i += 1
                else:
                    g.write(line)
            
            # insert remaining terms at the end
            while i < total_new:
                term, posting = terms[i]
                g.write(term + ":" + json.dumps(posting) + "\n")
                i += 1
        
        os.replace(temp_path, index_path)
    PARTIAL_INDEX = []
    

def write_report():
    global DOCID
    tokens = 0
    size = 0
    # for each index, add the size and count index
    for index in "0123456789abcdefghijklmnopqrstuvwxyz":
        path =  "indexes/" + index + ".txt"
        try:
            size += os.path.getsize(path)
            tokens += sum(1 for _ in open(path, "rb"))
        except (FileNotFoundError, OSError):
            pass
    size = size / 1024

    with open("report.txt", "w") as f:
        f.write(f"Indexed documents: {DOCID}\n")
        f.write(f"Unique Tokens: {tokens}\n")
        f.write(f"Total size: {size:.2f} KB\n")


if __name__ == "__main__":
    data_path = os.path.join(os.getcwd(), "DEV" if DEV else "ANALYST")
    try:
        os.remove("docids.txt")
        shutil.rmtree("indexes")
    except FileNotFoundError:
        pass
    initialize_index(data_path)
