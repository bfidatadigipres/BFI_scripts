# Imports
import os
import sqlite3
from elasticsearch import Elasticsearch

# Variables
DATABASE = os.environ.get('DATABASE_TRANSCODE')

# Connect to elasticsearch and SQlite3
ES_SEARCH = os.environ.get('ES_SEARCH_PATH')
ES = Elasticsearch([ES_SEARCH])
conn = sqlite3.connect(DATABASE)

# Fetch all records
cursor = conn.execute('SELECT name, email, download_type, fname, download_path, fpath, transcode, status, date FROM DOWNLOADS')
records = cursor.fetchall()

# Index each record to the dpi_downloads elasticsearch index
for record in records:
    es.index(index='dpi_downloads', body={
        "name": record[0],
        "email": record[1],
        "download_type": record[2],
        "fname": record[3],
        "download_path": record[4],
        "fpath": record[5],
        "transcode": record[6],
        "status": record[7],
        "date": record[8]
    })

# Close connection
conn.close()