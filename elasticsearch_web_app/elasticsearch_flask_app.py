import os
from functools import lru_cache

import elasticsearch_helper as elastic_helper
import pandas as pd
from elasticsearch import Elasticsearch
from flask import Flask, render_template, request

app = Flask(__name__, template_folder="templates/")

ELASTIC_PASS = os.environ["ELASTIC_PASS"]
ELASTIC_PATH = os.environ["ELASTIC_PATH"]

es = Elasticsearch(
    ELASTIC_PATH, basic_auth=("elastic", ELASTIC_PASS), verify_certs=False
)

autoingest_index = []


@app.route("/")
def home():
    filename = request.args.get("query")
    autoingest_index = elastic_helper.get_autoingest_docs(es)
    df = elastic_helper.search_by_filepath(filename, autoingest_index, es)
    table_html = df.to_html(classes="table table-striped", index=False)
    return render_template("index.html", table_html=table_html)


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True, port=9000)
# N_10119416_01of01.ts
