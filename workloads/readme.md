## Setup environment

Only three packages: absl-py, numpy, pandas
```angular2html
pip install -r requirements.txt
```

## Download IMDB data
```angular2html
mkdir imdb && cd imdb && wget -c http://homepages.cwi.nl/~boncz/job/imdb.tgz && tar -xvzf imdb.tgz
python workloads/IMDB/prepend_imdb_headers.py --csv_dir /path/to/imdb
```

## Execute repeating analytical queries
```angular2html
cd workloads/IMDB_extended/

```