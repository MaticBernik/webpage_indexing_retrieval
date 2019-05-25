from pathlib import Path
import sqlite3
from bs4 import BeautifulSoup
import nltk
from stopwords import stop_words_slovene
import re
import string


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
    #text = soup.findAll(text=True)
    text = soup.get_text()
    return text

def extract_text_tokenize(html : str):
    #Extract text from html webpage
    text = extract_text(html)
    #Tokenize text
    tokens = nltk.word_tokenize(text)
    return tokens

def preprocess_document(html : str):
    tokens = extract_text_tokenize(html)
    #Enumerate tokens
    tokens = enumerate(tokens)
    #Remove tokens of lenght 1
    tokens = [token for token in tokens if len(token[1])>1]
    #Remove stopwords
    tokens = [token for token in tokens if token[1] not in stop_words_slovene]
    #Remove tokens consisting only of punctuations
    #punctuation_pattern = re.compile(r"\b["+string.punctuation+"]+\b")
    punctuation_pattern = re.compile(r"^[^\d^\s^\w]+$")
    tokens = [token for token in tokens if not punctuation_pattern.match(token[1])]
    #Swap all numbers with $NUMBER tag
    number_pattern = re.compile(r"^\d+[.,\d]*$")
    tokens = [(token[0],"$NUMBER") if number_pattern.match(token[1]) else token for token in tokens]
    #Swap every token containing '=' with $EQUALS tag
    tokens = [(token[0],"$EQUALS") if '=' in token else token for token in tokens]
    #Lowercase
    tokens = [(token[0],token[1].lower()) for token in tokens]
    return tokens


if __name__ == "__main__":
    print("***Connecting to database..")
    db_conn = connect_database()
    print("\t...done!")
    db_cursor = db_conn.cursor()
    print("***Reading webpages...")
    webpages = {webpage.name:webpage.read_text() for webpage in WEBPAGES_DIR.glob('**/*.html')}
    print("\t...done!")
    token_insert_statement='''INSERT INTO IndexWord(word) 
                                SELECT ?
                                WHERE NOT EXISTS(SELECT 1 FROM IndexWord WHERE word = ?);'''
    posting_insert_statement='''INSERT INTO Posting(word,documentName,frequency,indexes) 
                                SELECT ?,?,?,?
                                WHERE NOT EXISTS(SELECT 1 FROM Posting WHERE word = ? AND documentName = ?);'''                             
    print("***Processing and indexing webpages...")
    vocab=set()
    i_doc=1       
    for documentName,htmlContent in webpages.items():
        print("\t",str(i_doc)+" / "+str(len(webpages_processed)),'\t'+documentName)
        print("\t\tProcessing webpage..")
        documentContent = preprocess_document(htmlContent)
        print("\t\tBuilding index...")
        document_vocab = set(token[1] for token in documentContent)
        vocab=vocab.union(document_vocab)
        token_indices={w:[] for w in document_vocab}
        #Insert new tokens to word index 
        for token in document_vocab:
            db_cursor.execute(token_insert_statement,(token,token))
            db_conn.commit()
        #Find token indices    
        for idx,token in documentContent:    
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
    db_conn.close()      
            
            