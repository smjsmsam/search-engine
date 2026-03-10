Tokenize, index, search, rank.

# inverted index: map(token, postings)
    # does not hold all of index in memory
    # offloada hash map from main memory to
        # partial index on disk at >= 3x during index construction,
        # and merge all partial indexes at the end
        # which are split into seperate index files with term ranges

# posting: represent token's occurence in a document
    # variable-size
        # disk: continuous run of postings
        # memory: linked lists, variable-length arrays, associative arrays
    # document name/id found in
    # tf-idf score

# analytics:
    # number of indexed documents
    # number of unique tokens
    # total size (KB) of index on disk

# tokenizer -> text processing -> indexer

# tokenizer
    # remove html tags
        # note: some might not have html, some might broken HTML
    # split into words
    # words in bold, headings, and titles are more important

# text processing
    # normalize
    # porter stemming
    # for this project: no stop words

# indexer
    # term, doc, freq (terms and counts) -> posting lists (docIDs)
    # multiple term entries in single document are merged
    # split into dictionary and postings
    # add document frequency 
