"""
Landing page for this site must always be log in which checks
users database to validate username/password. Then proceed onto
the workflow request page.

Flask App for Workflow creation tool using SQLite database supply
Retrieve requests from HTML web input, update SQLite db with
new requests, using POST. Accessed by Python record creation script
using GET requests, and statuses/prirefs updated when records made.

Work in progress
2025
"""

import datetime
import os
import re
import sqlite3

from flask import Flask, render_template, request

app = Flask(__name__)


@app.route("/")
@app.route("/home")
def index():
    return render_template("index.html")


DBASE = os.environ.get("DATABASE_TRANSCODE")
CONNECT = sqlite3.connect(DBASE)
CONNECT.execute(
    "CREATE TABLE IF NOT EXISTS DOWNLOADS (name TEXT, email TEXT, download_type TEXT, fname TEXT, download_path TEXT, fpath TEXT, transcode TEXT, status TEXT, date TEXT)"
)
FLASK_HOST = os.environ["FLASK_HOST"]


@app.route("/dpi_download_request", methods=["GET", "POST"])
def dpi_download_request():
    """
    Handle incoming path containing video
    reference_number as last element
    """

    if request.method == "GET":
        fname = request.args.get("file")
        transcode = request.args.get("option")
        if fname and transcode:
            return render_template(
                "initiate2_transcode.html", file=fname, trans_option=transcode
            )

    if request.method == "POST":
        name = request.form["name"].strip()
        email = request.form["email"].strip()
        download_type = request.form["download_type"].strip()
        fname = request.form["fname"].strip()
        download_path = request.form["download_path"].strip()
        fpath = request.form["fpath"].strip()
        transcode = request.form["transcode"].strip()
        # Filter out non alphanumeric / underscores from fname
        fpath = re.sub("\W+", "", fpath)
        status = "Requested"
        date_stamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # Check for non-BFI email and reject
        if "bfi.org.uk" not in email:
            return render_template("email_error_transcode.html")
        with sqlite3.connect(DBASE) as users:
            cursor = users.cursor()
            cursor.execute(
                "INSERT INTO DOWNLOADS (name,email,download_type,fname,download_path,fpath,transcode,status,date) VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    name,
                    email,
                    download_type,
                    fname,
                    download_path,
                    fpath,
                    transcode,
                    status,
                    date_stamp,
                ),
            )
            users.commit()
        return render_template("index_transcode.html")
    else:
        return render_template("initiate_transcode.html")


@app.route("/dpi_download")
def dpi_download():
    """
    Return the View all requested page
    """
    connect = sqlite3.connect(DBASE)
    cursor = connect.cursor()
    cursor.execute("SELECT * FROM DOWNLOADS where date >= datetime('now','-14 days')")
    data = cursor.fetchall()
    return render_template("downloads_transcode.html", data=data)


if __name__ == "__main__":
    app.run(host=FLASK_HOST, debug=False, port=5500)
