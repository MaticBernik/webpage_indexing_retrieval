from index import preprocess_document
from pathlib import Path
import sqlite3
from typing import Sequence

DATA_DIR = Path('DATA/')
WEBPAGES_DIR = DATA_DIR / 'webpages/'
SQLITE_PATH = DATA_DIR / 'webpage_index.db'
QUERIES = ["predelovalne dejavnosti","trgovina","social services"]


def query_naive(query : Sequence[str]):
    pass

def query_indexed(query : Sequence[str]):
    indices = retrieve_indices(query)
    print(indices)

def retrieve_indices(query : Sequence[str]):
    posting_select_statement = """SELECT documentName,word,frequency,indexes FROM Posting WHERE word = ?;"""
    conn = sqlite3.connect(str(SQLITE_PATH))
    cursor = conn.cursor()
    postings=[]
    for word in query:
        cursor.execute(posting_select_statement,(word,))
        postings+=cursor.fetchall()
    indices_by_files={}
    for p in postings:
        if p[0] not in indices_by_files:
            indices_by_files[p[0]]=[]
        indices_by_files[p[0]].append(p)
    return indices_by_files    

if __name__ == "__main__":
    #Preprocess queries 
    QUERIES = [preprocess_document(q) for q in QUERIES]
    for query in QUERIES:
        if SQLITE_PATH.exists():
            query_result = query_indexed(query)
        else:
            query_result = query_naive(query)
        print(query_result)
        
    