# HTML DATA indexing and retrieval

Implementation of a simple (html) document index of a provided corpus with the support for querying against it.
The results of the indexing operation are stored in an SQLite database.
Retrieval operation looks up indexes in a raw unprocessed documents and provides it's context.


## Webpage domains indexed
* e-prostor.gov.si
* e-uporava.gov.si
* evem.gov.si
* podatki.gov.si

## Repository structure
Main code resides within [build_index.py](CODE/build_index.py) and [retrieve_queries.py](CODE/retrieve_queries.py).

Directory [DATA/query_results/](DATA/query_results/) holds .txt files containing sorted outputs of the query retrieval results. Tags DB and RAW annotate either indexing via built database or manual scanning of the corpus files.  

## Installation 
### Prerequisites
Python 3.6/3.7 (tested on linux - Ubuntu 18.04)
Packages used:

* lxml
* pathlib
* BeautifulSoup
* nltk
* sqlite3
* re, string

## Running
Run [build_index.py](CODE/build_index.py) first in order to build the SQLite index of the corpus. This is required in order to achieve time efficient retrieval. This step can also be skipped when using manual scanning of the corpus files.

Run [retrieve_queries.py](CODE/retrieve_queries.py) to find the occurence of query in the corpus files. Custom queries can be appended to the __QUERIES__ variable in [retrieve_queries.py](CODE/retrieve_queries.py).

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details
