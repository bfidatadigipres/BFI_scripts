'''
Flask app for web front to elasticsearch for DPI downloading
Broadcasting to
https://bfinationalarchiverequest.bfi.org.uk/dpi_download

Joanna White
2023
'''

import os
import re
import datetime
from elasticsearch import Elasticsearch
from flask import Flask, render_template, request

app = Flask(__name__)

@app.route('/')
@app.route('/home')
def index():
    return render_template('index_transcode.html')

ES_SEARCH = os.environ.get('ES_SEARCH_PATH')
ES = Elasticsearch([ES_SEARCH])

if ES.ping():
    print("Connected to Elasticsearch")
else:
    print("Something's wrong")


@app.route('/dpi_download_request', methods=['GET', 'POST'])
def dpi_download_request():
    '''
    Handle incoming path containing video
    reference_number as last element
    '''

    if request.method == 'GET':
        fname = request.args.get("file")
        transcode = request.args.get("option")
        if fname and transcode:
            return render_template('initiate2_transcode.html', file=fname, trans_option=transcode)

    if request.method == 'POST':
        name = request.form['name'].strip()
        email = request.form['email'].strip()
        download_type = request.form['download_type'].strip()
        fname = request.form['fname'].strip()
        download_path = request.form['download_path'].strip()
        fpath = request.form['fpath'].strip()
        transcode = request.form['transcode'].strip()
        # Filter out non alphanumeric / underscores from fname
        fpath = re.sub('\W+', '', fpath)
        status = 'Requested'
        date_stamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # Check for non-BFI email and reject
        if 'bfi.org.uk' not in email:
            return render_template('email_error_transcode.html')
        ES.index(index='dpi_downloads', document={
            "name": name,
            "email": email,
            "download_type": download_type,
            "fname": fname,
            "download_path": download_path,
            "fpath": fpath,
            "transcode": transcode,
            "status": status,
            "date": date_stamp
        })
        return render_template('index_transcode.html')
    else:
        return render_template('initiate_transcode.html')


@app.route('/dpi_download')
def dpi_download():
    '''
    Return the View all requested page
    '''
    search_results = ES.search(index='dpi_downloads', query={'range': {'date': {'gte': 'now-14d/d', 'lte': 'now/d'}}}, size=500)
    data = []
    for row in search_results['hits']['hits']:
        record = [(value) for key, value in row['_source'].items()]
        data.append(tuple(record))

    return render_template("downloads_transcode.html", data=data)


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=False, port=5500)
