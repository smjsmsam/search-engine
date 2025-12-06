import sys
import time
from search import search_query


if __name__ == "__main__":
    '''
    python terminal.py
    '''
    print("Ready.")
    print("Enter 'exit' to exit.")

    while True:
        response = input("Search: ")
        if response.lower() == 'exit':
            break
        start = time.time()
        urls = search_query(response)
        end = time.time()
        if urls:
            print(*urls, sep="\n")
        else:
            print("No result found")
        print(f'Took {end-start} seconds to gather {len(urls)} responses.')