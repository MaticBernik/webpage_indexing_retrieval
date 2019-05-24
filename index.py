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
    #webpages_processed = {key:preprocess_document(content) for key,content in Swebpages.items()}
    webpages_processed = {key:preprocess_document(content) for key,content in list(webpages.items())[:10]}
    print("\t...done!")
    for name,content in webpages_processed.items():
        c = Counter(content)
        token_insert_statement='''INSERT INTO IndexWord(word) 
                                SELECT ?
                                WHERE NOT EXISTS(SELECT 1 FROM IndexWord WHERE word = ?);'''
        for word in c:
            db_cursor.execute(token_insert_statement,(word,word))
            db_conn.commit()
        