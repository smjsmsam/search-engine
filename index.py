# inverted index: map(token, postings)
    # cannot hold all of index in memory
    # must offload hash map from main memory to
        # partial index on disk at >= 3x during index construction,
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

# tokenizer
    # remove html tags
        # note: some might not have html, some might broken HTML
    # split into words
    # words in bold, headings, and titles are more important

# text processing
    # normalize
    # stemming
        # suggested: porter stemming
    # note: no stop words

# indexer
    # from slides: term, doc, freq (terms and counts) -> posting lists (docIDs)
    # multiple term entries in single document are merged
    # split into dictionary and postings
    # add document frequency 
