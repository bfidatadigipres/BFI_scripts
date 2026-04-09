"""
email_sender_dpi/app.py
"""

import logging
import os
import sys
import re
from pathlib import Path

from flask import Flask, flash, render_template, request

sys.path.append(os.environ["CODE"])
import utils

app = Flask(__name__, template_folder="templates")
HOST = os.environ["HOST"]
PORT = os.environ["PORT"]
LOG = os.environ["EMAIL_LOG"]
ALLOWED_BASE_PATH = os.environ.get("ALLOWED_BASE_PATH", "/mnt/bp_nas/admin/email_docs/")
ALLOWED_EXTENSIONS = {
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".pdf",
    ".zip",
    ".csv",
    ".doc",
    "docx",
    ".ods",
    "",
}


# Configure logging
logger = logging.getLogger("flask_logger")
hdlr = logging.FileHandler(LOG)
formatter = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)


app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024
app.secret_key = os.environ["flask_key"]

if not app.secret_key:
    raise ValueError("flask_key environment variable must be set")


def is_safe_path(filepath):
    try:
        # Convert to absolute path and resolve any symlinks/relative components
        requested_path = Path(filepath).resolve()
        base_path = Path(ALLOWED_BASE_PATH).resolve()

        # Check if the resolved path is within the allowed base path
        if not str(requested_path).startswith(str(base_path)):
            return False, "Access denied: File must be within allowed directory"

        # Check if file exists
        if not requested_path.exists():
            return False, "File not found"

        # Check if it's a file (not a directory)
        if not requested_path.is_file():
            return False, "Path must point to a file"

        max_size = 6600 * 1024 * 1024
        file_size = requested_path.stat().st_size
        if file_size > max_size:
            return (
                False,
                f"File size ({file_size / (1024*1024):.2f}MB) exceeds maximum allowed size (660MB)",
            )

        return True, None

    except (ValueError, OSError) as e:
        logger.error(f"Path validation error: {str(e)}")
        return False, "Invalid file path"


def is_valid_email(email):
    """Validate email format and domain."""
    pattern = r"^[a-zA-Z0-9._%+-]+@bfi\.org\.uk$"
    return re.match(pattern, email) is not None


def validate_input(email, subject, body, image_path):
    """
    Validate all user inputs.

    Returns:
    --------
    tuple: (is_valid: bool, error_message: str or None)
    """
    # Validate email
    if not email:
        return False, "Email address is required"

    if not is_valid_email(email):
        return False, "Invalid email address. Must be a valid @bfi.org.uk email"

    # Validate subject
    if not subject or not subject.strip():
        return False, "Subject is required"

    if len(subject) > 200:
        return False, "Subject must be 200 characters or less"

    if len(body) > 10000:
        return False, "Email body must be 10,000 characters or less"

    # Validate file path
    if not image_path or not image_path.strip():
        return False, "File path is required"

    is_valid, error = is_safe_path(image_path)
    if not is_valid:
        return False, error

    return True, None


@app.route("/", methods=["GET"])
def index():
    """
    Renders the index page for the email sender application.

    Parameters:
    -----------
    None

    Returns:
    --------
    render_template(string): Renders the index page or error page based on the outcome. None if an error occurs.
    """
    return render_template("index.html")


@app.route("/", methods=["POST", "GET"])
def send_email_with_image():
    """
    Handles the email sending functionality.

    Parameters:
    -----------
    None

    Returns:
    --------
    render_template(string) | None: Renders the index page or error page based on the outcome. None if an error occurs.
    """
    try:
        email = request.form.get("email", "").strip()
        subject = request.form.get("subject", "").strip()
        body = request.form.get("body", "").strip()
        image_path = request.form.get("file", "").strip()

        # Validate all inputs
        is_valid, error_message = validate_input(email, subject, body, image_path)
        if not is_valid:
            logger.warning(f"Validation failed for {email}: {error_message}")
            flash(error_message, "error")
            return render_template("error_page.html"), 400
        # Send email
        success, outcome = utils.send_email(email, subject, body, image_path)

        if outcome:
            logger.error(f"File size exceeded for {email}")
            flash("File size exceeds the maximum allowed limit", "error")
            return render_template("error_page.html"), 400

        if success:
            logger.info(f"Email successfully sent to {email}")
            flash(f"Email successfully sent to {email}", "success")
            return render_template("index.html"), 200
        else:
            logger.error(f"Failed to send email to {email}")
            flash("Failed to send email. Please try again later.", "error")
            return render_template("error_page.html"), 500

    except Exception as e:
        logger.critical(f"Unexpected error: {str(e)}", exc_info=True)
        flash("An unexpected error occurred. Please contact support.", "error")
        return render_template("error_page.html")


@app.errorhandler(413)
def request_entity_too_large(error):
    """Handle file size too large errors."""
    flash("Request too large. Maximum size is 660MB.", "error")
    return render_template("error_page.html"), 413


@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors."""
    return render_template("error_page.html"), 404


@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors."""
    logger.error(f"Internal server error: {str(error)}")
    return render_template("error_page.html"), 500


if __name__ == "__main__":
    app.run(host=HOST, debug=False, port=PORT, use_reloader=False)
