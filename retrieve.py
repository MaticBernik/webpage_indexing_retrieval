from index import preprocess_document, extract_text, extract_text_tokenize
from pathlib import Path
from stopwords import stop_words_slovene
import sqlite3
import nltk
from typing import Sequence

DATA_DIR = Path('DATA/')
WEBPAGES_DIR = DATA_DIR / 'webpages/'
SQLITE_PATH = DATA_DIR / 'webpage_index.db'
# QUERIES = ["predelovalne dejavnosti", "trgovina", "social services"]
QUERIES = ["proizvodnja dejavnosti", "trgovina", "social services"]


def query_naive(query: Sequence[str]):
    pass
    return None


def query_indexed(query: Sequence[str]):

    indices = retrieve_indices(query)

    retrieved_data = []
    for document in indices:

        freq = 0
        indices_str = ""
        for query_token in indices[document]:
            freq += query_token[2]  # token frequency
            indices_str += query_token[3]  # toekn indices

        hit_indices = [int(x) for x in indices_str.split(',')]
        hit_indices = sorted(list(set(hit_indices)))

        retrieved_data.append((freq, document, hit_indices))

    return retrieved_data


def get_document_tokenized(path_html):

    with open(path_html, "r") as f:
        html = f.read()

    html_text = extract_text(html)
    text = ' '.join(html_text)
    tokens = nltk.word_tokenize(text)

    # TODO: COMMENT OUT THIS PART ( SHOULD BE REGARDIN THE ORIGINAL FILE)
    #Remove stopwords
    tokens = [token for token in tokens if token not in stop_words_slovene]
    #Lowercase
    tokens = [token.lower() for token in tokens]
    return tokens

def display_query_results(retrieved_data, window_size=3):

    retrieved_data = sorted(retrieved_data, reverse=True)

    print("Freq.        Document                                  Snippet")
    print("-----------  ----------------------------------------- -----------------------------------------------------------")
    for document in retrieved_data:

        folder_name = ".".join(document[1].split('.')[:3])
        document_path = WEBPAGES_DIR / folder_name / document[1]

        doc_tokens = get_document_tokenized(document_path)
        # hits_string = " ".join([str(i) for i in document[2]])

        hits_string = ""

        for i in document[2]:

            start_indx = 0 if i - window_size < 0 else i - window_size
            end_indx = len(doc_tokens) if i + window_size > len(doc_tokens) else i + window_size

            if end_indx == len(doc_tokens):
                hits_string += " ".join(doc_tokens[start_indx:end_indx])
            else:
                hits_string += " ".join(doc_tokens[start_indx:end_indx])
                hits_string += " ... "

            print(hits_string)
        

        print("{:12} {:42} {:60}".format(str(document[0]), document[1], hits_string))


def retrieve_indices(query: Sequence[str]):

    posting_select_statement = """SELECT documentName,word,frequency,indexes FROM Posting WHERE word = ?;"""
    conn = sqlite3.connect(str(SQLITE_PATH))
    cursor = conn.cursor()

    postings = []
    for word in query:
        cursor.execute(posting_select_statement, (word, ))
        postings += cursor.fetchall()

    indices_by_files = {}
    for p in postings:
        if p[0] not in indices_by_files:
            indices_by_files[p[0]] = []
        indices_by_files[p[0]].append(p)
    return indices_by_files


if __name__ == "__main__":

    #Preprocess queries
    QUERIES = [preprocess_document(q) for q in QUERIES]
    for query in QUERIES:
        print("QUERY: ")
        print(query)
        if SQLITE_PATH.exists():
            query_results = query_indexed(query)
        else:
            query_results = query_naive(query)

        display_query_results(query_results)

        print("==============================================================================================================================")
        print("//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////")
        print("==============================================================================================================================")
