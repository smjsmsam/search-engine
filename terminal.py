import sys
import time
from search import search_query


if __name__ == "__main__":
    # for terminal search
    if len(sys.argv) != 2:
        print("Invalid number of arguments! Example: python terminal.py 'cristina lopes'")
        sys.exit(1)
    query = sys.argv[1]
    start = time.time()
    urls = search_query(query)
    end = time.time()
    print(url for url in urls)
    print(f'{end-start} seconds')