#!/usr/bin/env python3

'''
Flask app for collection of data
from HTML dpi_requests.html forwarding
to SQLite3 database for retrieval by
python code which organises move of data.

Joanna White
2023
'''

# Imports
import os
import re
import sqlite3
import datetime
import itertools
from flask import Flask, render_template, request
from elasticsearch import Elasticsearch

# Initiate Flask app / Elastic search
app = Flask(__name__)
@app.route('/')
@app.route('/home')
def index():
    return render_template('index.html')

ES_SEARCH = os.environ.get('ES_SEARCH_PATH')
ES = Elasticsearch([ES_SEARCH])

if ES.ping():
    print("Connected to Elasticsearch")
else:
    print("Something's wrong")

# Global variables / connect or create database.db
DBASE = os.environ['DATABASE_NEWS_PRESERVATION']
CONNECT = sqlite3.connect(DBASE)
CONNECT.execute('CREATE TABLE IF NOT EXISTS DOWNLOADS (name TEXT, email TEXT, preservation_date TEXT, channel TEXT, status TEXT, date TEXT)')


def date_gen(date_str):
    '''
    Generate date for checks if date
    in correct 14 day window
    '''
    from_date = datetime.date.fromisoformat(date_str)
    while True:
        yield from_date
        from_date = from_date - datetime.timedelta(days=1)


def check_date_range(preservation_date):
    '''
    Check date range is correct
    and that the file is not today
    '''
    date_range = []
    today_date = str(datetime.date.today())
    period = itertools.islice(date_gen(today_date), 14)
    for dt in period:
        date_range.append(dt.strftime('%Y-%m-%d'))

    daterange = ', '.join(date_range)
    print(f"Target range for DPI moves: {date_range}")
    print(preservation_date)
    if preservation_date in str(date_range):
        print("Requested date is in date range")
        return True

    return False


@app.route('/dpi_move_request', methods=['GET', 'POST'])
def dpi_move_request():
    '''
    Handle incoming path containing video
    reference_number as last element
    '''

    if request.method == 'POST':
        name = request.form['name'].strip()
        email = request.form['email'].strip()
        preservation_date = request.form['preservation_date'].strip()
        channel = request.form['channel'].strip()
        # Check manually entered date is valid for format/period
        pattern = "^20[0-9]{2}-[0-1]{1}[0-9]{1}-[0-3]{1}[0-9]{1}$"
        if not re.match(pattern, preservation_date):
            print("Failed regex check")
            return render_template('date_error.html')
        success = check_date_range(preservation_date)
        if not success:
            print("Failed check_date_range()")
            return render_template('date_error.html')
        status = 'Requested'
        date_stamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # Check for non-BFI email and reject
        if 'bfi.org.uk' not in email:
            return render_template('email_error.html')
        with sqlite3.connect(DBASE) as users:
            cursor = users.cursor()
            cursor.execute("INSERT INTO DOWNLOADS (name,email,preservation_date,channel,status,date) VALUES (?,?,?,?,?,?)", (name, email, preservation_date, channel, status, date_stamp))
            users.commit()
        return render_template('index.html')
    else:
        return render_template('initiate.html')


@app.route('/dpi_move')
def dpi_move():
    '''
    Return the View all requested page
    '''
    connect = sqlite3.connect(DBASE)
    cursor = connect.cursor()
    cursor.execute(f"SELECT * FROM DOWNLOADS where date >= datetime('now','-14 days')")
    data = cursor.fetchall()
    return render_template("dpi_requests.html", data=data)


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
