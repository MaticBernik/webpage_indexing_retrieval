from pathlib import Path
import sqlite3
from bs4 import BeautifulSoup
import nltk
from stopwords import stop_words_slovene
from collections import Counter


DATA_DIR = Path('DATA/')
WEBPAGES_DIR = DATA_DIR / 'webpages/'
SQL_SCRIPT_PATH = DATA_DIR / 'build_index.sql'
#STOPWORDS_SCRIPT = DATA_DIR / 'stopwords.py'
SQLITE_PATH = DATA_DIR / 'webpage_index.db'

def connect_database():
    db_exsists = SQLITE_PATH.exists()
    conn = sqlite3.connect(str(SQLITE_PATH))
    if not db_exsists:
        conn.cursor().executescript(SQL_SCRIPT_PATH.read_text())
    return conn

def extract_text(html : str):
    soup = BeautifulSoup(html, features="lxml")
    text = soup.findAll(text=True)
    return text

def preprocess_document(html : str):
    #Extract text from html webpage
    text = ' '.join(extract_text(html))
    #Tokenize text
    tokens = nltk.word_tokenize(text)
    #Remove stopwords
    tokens = [token for token in tokens if token not in stop_words_slovene]
    #Lowercase
    tokens = [token.lower() for token in tokens]
    return tokens


if __name__ == "__main__":
    print("***Connecting to database..")
    db_conn = connect_database()
    print("\t...done!")
    db_cursor = db_conn.cursor()
    print("***Reading webpages...")
    webpages = {webpage.name:webpage.read_text() for webpage in WEBPAGES_DIR.glob('**/*.html')}
    print("\t...done!")
    print("***Processing webpages...")
    webpages_processed = {key:preprocess_document(content) for key,content in Swebpages.items()}
    #webpages_processed = {key:preprocess_document(content) for key,content in list(webpages.items())[:10]}
    print("\t...done!")
    token_insert_statement='''INSERT INTO IndexWord(word) 
                                SELECT ?
                                WHERE NOT EXISTS(SELECT 1 FROM IndexWord WHERE word = ?);'''
    posting_insert_statement='''INSERT INTO Posting(word,documentName,frequency,indexes) 
                                SELECT ?,?,?,?
                                WHERE NOT EXISTS(SELECT 1 FROM Posting WHERE word = ? AND documentName = ?);'''                             
    vocab=set()
    i_doc=1
    print("***Building index...")
    for documentName,documentContent in webpages_processed.items():
        print("\t\t",str(i_doc)+" / "+str(len(webpages_processed)))
        document_vocab = set(documentContent)
        vocab=vocab.union(document_vocab)
        token_indices={w:[] for w in document_vocab}
        #Insert new tokens to word index 
        for token in document_vocab:
            db_cursor.execute(token_insert_statement,(token,token))
            db_conn.commit()
        #Find token indices    
        for idx,token in enumerate(documentContent):    
            token_indices[token].append(idx)
        #Insert new postings
        for token,indices in token_indices.items():
            db_cursor.execute(posting_insert_statement,
                              (token,
                               documentName,
                               len(indices),
                               ','.join([str(i) for i in indices]),
                               token,
                               documentName))
            db_conn.commit()
        i_doc+=1     
    print("\t...done!")        
            
            