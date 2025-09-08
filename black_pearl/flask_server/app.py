#! /usr/bin/env python3

"""
Flask web application runs 24/7 waiting for
Black Pearl notifications to arrive and confirm
successful PUT of data from new DPI BP scripts.

Outputs notification of arrival to shared log
and dumps data received to JSON file in Isilon Logs.

Stephen McConnachie
2022
"""

import json
import logging
import os

from flask import Flask, jsonify, render_template, request

LOG_PATH = os.environ["LOG_PATH"]
FLASK_HOST = os.environ["FLASK_HOST"]

# Configure logging
logging.basicConfig(
    filename=os.path.join(LOG_PATH, "black_pearl_move_put.log"),
    filemode="a",
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

# creating the flask app
app = Flask(__name__)


@app.route("/jobcompleted", methods=["POST"])
def postJsonHandler():
    """
    From https://techtutorialsx.com/2017/01/07/flask-parsing-json-data/
    """
    notification = request.get_json()
    print(notification)
    jobID = notification["Notification"]["Event"]["JobId"]
    json_filename = os.path.join(LOG_PATH, f"black_pearl/{jobID}.json")
    notification_string = json.dumps(notification)
    with open(json_filename, "w") as json_file:
        json_file.write(notification_string)
    return "JSON posted", 200
    logging.info("%s %s", notification_string, jobID)


if __name__ == "__main__":
    app.run(host=FLASK_HOST, debug=False, port=5000)
