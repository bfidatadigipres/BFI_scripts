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
import sys
import sqlite3
from contextlib import closing
from flask import Flask, render_template, request, redirect, url_for, session, abort, g, flash

sys.path.append(os.environ.get("CODE"))
import adlib_v3 as adlib
import utils

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY")

DBASE = os.environ.get("WF_DATABASE")
FLASK_HOST = os.environ.get("FLASK_HOST")
LOG_PATH = os.environ["LOG_PATH"]
CID_API = utils.get_current_api()

# Ensure DB and table exist
with sqlite3.connect(DBASE) as conn:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS REQUESTS (
            username TEXT NOT NULL,
            email TEXT NOT NULL,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            client_category TEXT NOT NULL,
            jobid INTEGER PRIMARY KEY AUTOINCREMENT,
            items_list TEXT NOT NULL,
            activity_code TEXT NOT NULL,
            request_type TEXT NOT NULL,
            request_outcome TEXT NOT NULL,
            description TEXT NOT NULL,
            delivery_date TEXT NOT NULL,
            destination TEXT NOT NULL,
            instructions TEXT,
            client_name TEXT NOT NULL,
            contact_details TEXT,
            department TEXT NOT NULL,
            status TEXT NOT NULL,
            date TEXT NOT NULL
        )
    """)


def get_user_data(username, password):
    """
    Request from CID usersdb
    Match supplied data
    """
    search = f"user_name='{username}'"
    try:
        result = adlib.retrieve_record(CID_API, "users", search, "1")[1]
    except (KeyError, IndexError, TypeError) as err:
        print(err)
        return []
    if not result:
        return []
    try:
        pwd = adlib.retrieve_field_name(result[0], "password")[0]
        status = adlib.retrieve_field_name(result[0], "user_status")[0]
    except (KeyError, IndexError):
        return []
    if status != "ACTIVE":
        return []
    if password == pwd:
        password = pwd = ""
        email = adlib.retrieve_field_name(result[0], "email_address")[0]
        fname = adlib.retrieve_field_name(result[0], "first_name")[0]
        lname = adlib.retrieve_field_name(result[0], "last_name")[0]
        dept = adlib.retrieve_field_name(result[0], "part_of")[0]
        activity_code = adlib.retrieve_field_name(result[0], "activity.code") 
        return [email, fname, lname, dept, activity_code]
    else:
        return []


def get_db():
    """
    One connection per request
    """
    if "db" not in g:
        g.db = sqlite3.connect(DBASE, detect_types=sqlite3.PARSE_DECLTYPES)
        g.db.row_factory = sqlite3.Row
    return g.db


@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def login_required(view_func):
    """
    Simple decorator to block unauthenticated access
    """
    from functools import wraps
    @wraps(view_func)
    def wrapped(*args, **kwargs):
        if not session.get("username"):
            flash("Please log in first.")
            return redirect(url_for("index", next=request.path))
        return view_func(*args, **kwargs)
    return wrapped


@app.route("/", methods=["GET", "POST"])
@app.route("/home", methods=["GET", "POST"])
def index():
    """
    Set up logging page
    """
    if request.method == "POST":
        uname = (request.form.get("username") or "").strip().lower()
        password = request.form.get("password") or ""
        if not uname or not password:
            return render_template("login.html", error="User name and password are required.")
        user_data = get_user_data(uname, password)
        if user_data == []:
            # No confirmation of failed credential here
            return render_template("login.html", error="Invalid credentials.")
        session["username"] = uname
        session["email"] = user_data[0]
        session["first_name"] = user_data[1]
        session["last_name"] = user_data[2]
        session["department"] = user_data[3]
        session["activity_codes"] = user_data[4]
        # Redirect to second form after login
        nxt = request.args.get("next") or url_for("workflow_request")
        return redirect(nxt)

    # If already logged in go to second form
    if session.get("username"):
        return redirect(url_for("workflow_request"))

    return render_template("login.html")


@app.route("/logout", methods=["POST"])
def logout():
    session.clear()
    return redirect(url_for("index"))


@app.route("/workflow_request", methods=["GET", "POST"])
@login_required
def workflow_request():
    """
    Authenticated entry for initiating a workflow request.
    Carry the logged-in details automatically into the form and POST.
    """

    # Retrieve all necessary user data from the session
    user_email = session.get("email")
    user_name = session.get("username")
    user_first_name = session.get("first_name")
    user_last_name = session.get("last_name")
    user_dept = session.get("department")
    # List of codes we need for the dropdown
    activity_codes_list = session.get("activity_codes", []) 
    if len(activity_codes_list) == 1 and activity_codes_list[0] is None:
        flash("Submission blocked: You do not have any valid Activity Codes assigned.\nPlease contact CollectionsSystems.", 'error')

    if request.method == "GET":
        # Pass user data to the template
        return render_template(
            "workflow_form.html",
            user_email=user_email,
            user_name=user_name,
            user_first_name=user_first_name,
            user_last_name=user_last_name,
            user_dept=user_dept,
            activity_codes=activity_codes_list
        )

    if request.method == "POST":
        if len(activity_codes_list) == 1 and activity_codes_list[0] is None:
            flash("Submission failed: Cannot submit request without a valid Activity Code.", 'error')
            return redirect(url_for("workflow_request"))

        # Force email/name to session value
        email = user_email
        username = user_name
        fname = user_first_name
        lname = user_last_name
        dept = user_dept
        
        # Get submitted data
        user_category = request.form.get("client_category", "").strip()
        saved_search = request.form.get("items_list", "").strip()
        
        # Retrieve selected activity code / text data / force status and date
        activity_code_selected = request.form.get("activity_code", "").strip() 
        if not activity_code_selected:
            flash("Submission failed: Please select an Activity Code.", 'error')
            return redirect(url_for("workflow_request"))
        request_type = request.form.get("request_type", "").strip()
        request_outcome = request.form.get("request_outcome", "").strip()
        description = request.form.get("description", "").strip()
        delivery_date = request.form.get("delivery_date", "").strip()
        destination = request.form.get("destination", "").strip()
        instructions = request.form.get("instructions", "").strip()
        client_name = request.form.get("client_name", "").strip()
        contact_details = request.form.get("contact_details", "").strip()
        status = "Requested"
        date_stamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if "bfi.org.uk" not in email:
            # Flash an error message with category 'error'
            flash("Email domain check failed. Please ensure you are using a BFI email address.", 'error')
            return redirect(url_for("workflow_request")) 
        
        try:
            with closing(get_db()) as db:
                db.execute(
                    "INSERT INTO REQUESTS (username,email,first_name,last_name,client_category,items_list,activity_code,request_type,request_outcome,description,delivery_date,destination,instructions,client_name,contact_details,department,status,date) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (username, email, fname, lname, user_category, saved_search, activity_code_selected, request_type, request_outcome, description, delivery_date, destination, instructions, client_name, contact_details, dept, status, date_stamp),
                )
                db.commit()
            
            # Flash a success message
            flash("Workflow request successfully submitted!", 'success')
            
            # Redirect back to the GET version of the workflow form
            return redirect(url_for("workflow_request"))

        except Exception as e:
            print(f"Database insertion failed: {e}")
            flash(f"An error occurred while submitting the request. Please try again.", 'error')
            return redirect(url_for("workflow_request"))

    # Fallback if needed
    abort(405)


if __name__ == "__main__":
    # NOTE: For production, set debug=False and serve behind a WSGI server (gunicorn/uwsgi)
    app.run(host=FLASK_HOST, debug=False, port=7860)
