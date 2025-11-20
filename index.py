import os
import shutil
import json
import re
import csv
from lxml import html, etree
from nltk.stem import PorterStemmer
import atexit


DEV = True
PARTIAL_INDEX = []
PARTIAL_LIST = []
CSVROWS = []
DOCID = 0
PS = PorterStemmer()
POSTING_COUNT = 0
POSTING_THRESHOLD = 1_000_000


def initialize_index(data_path):
    '''
    for each file in each domain, process the contents

    creates new partial index after a certain amount of postings
    '''
    global DOCID, POSTING_COUNT, POSTING_THRESHOLD, PARTIAL_INDEX

    # create folder to hold partial indexes
    os.makedirs("partials", exist_ok=True)
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
            CSVROWS.append([DOCID, file_info["url"]])

            tokens = tokenize(raw_text)
            terms = process_tokens(tokens)
            postings = create_postings(terms)

            PARTIAL_INDEX.extend(postings.items())
            POSTING_COUNT += len(postings)

            # offload partial index (postings)
            if POSTING_COUNT >= POSTING_THRESHOLD:
                offload_partial()
                POSTING_COUNT = 0


def tokenize(raw_text):
    '''
    remove html tags
    #TODO: Keep track of word position

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

    important_weights = frequencies(terms["important"])
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


def offload_partial():
    '''
    save PARTIAL_INDEX to json file
    '''
    global PARTIAL_INDEX, PARTIAL_LIST

    # dump to some file
    filepath = "partials/" + str(len(PARTIAL_LIST)+1) + ".json"
    with open (filepath, "w") as f:
        # check json compatibility, convert to regular dict for JSON
        json.dump(PARTIAL_INDEX, f)
    PARTIAL_LIST.append(filepath)
    PARTIAL_INDEX = []  # reset partial index
    print(f"Offloaded partial index #{len(PARTIAL_LIST)-1} with {len(PARTIAL_INDEX)} terms")


def merge_partial():
    '''
    sorts postings in each partial and updates index
    '''
    # create index files
    os.makedirs("indexes", exist_ok=True)
    for index in "0123456789abcdefghijklmnopqrstuvwxyz":
        path =  "indexes/" + index + ".txt"
        open(path, 'w').close()
    # merge each partial file
    for file in PARTIAL_LIST:
        with open(file, "r") as f:
            # sort
            content = json.load(f)
            sorted_postings = sorted(content, key=lambda x: x[0])
            # insert into index
            update_index(list(sorted_postings))


def update_index(postings):
    '''
    PARTIAL_INDEX = [{"[term]": {"document_id": [int],
     "freq": {"important": [int], "stuff": [int]}}}]
    '''
    letters = {}

    # divide list into letters, each term with a list of postings
    # letters = {"a": {"apple": [posting1, posting2, ...]}}
    for term, posting in postings:
        if term[0] not in letters:
            letters[term[0]] = {}
            letters[term[0]][term] = [posting]
        else:
            if term not in letters[term[0]]:
                letters[term[0]][term] = [posting]
            else:
                letters[term[0]][term].append(posting)

    # for each letter, add the new postings
    for letter, terms_dict in letters.items():
        terms = list(terms_dict.items())
        i = 0
        total_new = len(terms)

        index_path = "indexes/" + letter + ".txt"
        temp_path = index_path + ".tmp"

        # insert into temporary copy
        with open(index_path, 'r') as f, open(temp_path, 'w') as g:
            # file format is term:[{posting}, {posting}, ...]
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
                    current_postings = json.loads(current_list)
                    merged = current_postings + posting
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
    

def writecsv(filepath, rows):
    '''
    writes rows into filepath with the .csv file extension
    '''
    with open(filepath, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(["DOCID", "URL"])  # write header
        csvwriter.writerows(rows)


def write_report():
    '''
    save analytics to file
    '''
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


@atexit.register
def last_report():
    '''
    at the end of program, create document url map, combine partials, and create index files
    '''
    global CSVROWS
    writecsv("docids.csv", CSVROWS)
    offload_partial()
    print("Merging partials")
    merge_partial()
    write_report()
    create_index_of_index()


if __name__ == "__main__":
    '''
    index /DEV or /ANALYST, clearing previous items before indexing
    '''
    data_path = os.path.join(os.getcwd(), "DEV" if DEV else "ANALYST")
    try:
        os.remove("docids.csv")
        shutil.rmtree("indexes")
        shutil.rmtree("partials")
    except FileNotFoundError:
        pass
    initialize_index(data_path)
