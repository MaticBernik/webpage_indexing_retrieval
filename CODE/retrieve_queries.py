from build_index import preprocess_document, extract_text_tokenize
from pathlib import Path
from stopwords import stop_words_slovene
import sqlite3
import nltk
from typing import Sequence
import time

DATA_DIR = Path('DATA/')
WEBPAGES_DIR = DATA_DIR / 'webpages/'
QUERY_RESULTS_DIR = DATA_DIR / 'query_results/'
SQLITE_PATH = DATA_DIR / 'webpage_index.db'
QUERIES = ["predelovalne dejavnosti", "trgovina", "social services"]
QUERIES.extend(["finance borza trg", "podatki analiza znanost", "zakon", "kataster zemljišče davek meja ocena vlada stavba model register javni"])

def query_naive(query: Sequence[str]):
    webpages = list(WEBPAGES_DIR.glob('**/*.html'))
    
    index_tree = {}
    query_list = [ q for (_, q)  in query]
    

    for webpage in webpages:
        tokenized_doc = get_document_tokenized(webpage, raw=True)
        
        for (t_i, token) in tokenized_doc:
            
            if token in query_list:
                web_name = str(webpage.name)

                if web_name not in index_tree:
                    index_tree[web_name] = [1, web_name, [t_i]]
                else:
                    index_tree[web_name][0] += 1
                    index_tree[web_name][2].append(t_i)

    return list(index_tree.values())


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


def get_document_tokenized(path_html, raw=False):

    html = path_html.read_text()

    if raw:
        tokens = preprocess_document(html)
    else:
        tokens = extract_text_tokenize(html)

    return tokens


def gather_query_results(retrieved_data, window_size=3):

    retrieved_data = sorted(retrieved_data, reverse=True, key=lambda x: x[0])
    results = []

    for d_i, document in enumerate(retrieved_data):

        folder_name = ".".join(document[1].split('.')[:3])
        document_path = WEBPAGES_DIR / folder_name / document[1]
        doc_tokens = get_document_tokenized(document_path)

        hits_string = "..."

        for i in document[2]:

            start_indx = 0 if i - window_size < 0 else i - window_size
            end_indx = len(doc_tokens) if i + window_size + 1 > len(doc_tokens) else i + window_size + 1

            if end_indx == len(doc_tokens):
                hits_string += " ".join(doc_tokens[start_indx:end_indx])
            else:
                hits_string += " ".join(doc_tokens[start_indx:end_indx])
                hits_string += " ... "

        doc_hits = "{:12} {:42} {:60}".format(str(document[0]), document[1], hits_string)
        results.append(doc_hits)

    return results


def write_query_results(query, query_time, query_results, lookup_type):

    output = []
    str_query = "_".join([str(word) for (_, word) in query ])
    header_query = "Results for a query: \"" + str_query + "\""
    header_stats = "Results found in " + str(query_time) + "s"
    header_title = "Freq.        Document                                  Snippet"
    header_break = "-----------  ----------------------------------------- -----------------------------------------------------------"

    output.extend([header_query, "", header_stats, "", header_title, header_break])
    output.extend(query_results)
    
    output_path = str(QUERY_RESULTS_DIR) + "/" + lookup_type + "-" + str_query+ ".txt"
    print("Writing query results to: ", output_path)
    with open(output_path, 'w') as f:
        f.write("\n".join(output))
    

def display_query_results_limited(query_results):

    for row in query_results:
        print(row[:300])


def display_query_results_raw(retrieved_data, window_size=3):

    retrieved_data = sorted(retrieved_data, reverse=True)

    print("Freq.        Document                                  Snippet")
    print("-----------  ----------------------------------------- -----------------------------------------------------------")
    for document in retrieved_data:

        folder_name = ".".join(document[1].split('.')[:3])
        document_path = WEBPAGES_DIR / folder_name / document[1]

        doc_tokens = get_document_tokenized(document_path)
        # hits_string = " ".join([str(i) for i in document[2]])

        hits_string = "..."

        for i in document[2]:

            start_indx = 0 if i - window_size < 0 else i - window_size
            end_indx = len(doc_tokens) if i + window_size + 1 > len(doc_tokens) else i + window_size + 1

            if end_indx == len(doc_tokens):
                hits_string += " ".join(doc_tokens[start_indx:end_indx])
            else:
                hits_string += " ".join(doc_tokens[start_indx:end_indx])
                hits_string += " ... "

            # print(hits_string)
        
        print("{:12} {:42} {:60}".format(str(document[0]), document[1], hits_string))


def retrieve_indices(query: Sequence[str]):

    posting_select_statement = """SELECT documentName,word,frequency,indexes FROM Posting WHERE word = ?;"""
    conn = sqlite3.connect(str(SQLITE_PATH))
    cursor = conn.cursor()

    postings = []
    for (_, word) in query:
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
        print("Querying: ", query)

        start_t = time.time()
        if SQLITE_PATH.exists():
            lookup_type = "DB"
            query_results = query_indexed(query)
        else:
            lookup_type = "RAW"
            query_results = query_naive(query)

        end_t = time.time()

        lookup_results = gather_query_results(query_results)
        write_query_results(query, end_t-start_t, lookup_results, lookup_type)