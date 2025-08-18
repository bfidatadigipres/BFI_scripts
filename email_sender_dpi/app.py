import logging
import os
import sys

from flask import Flask, flash, render_template, request

sys.path.append(os.environ["CODE"])
import utils

app = Flask(__name__, template_folder="templates")
HOST = os.environ["HOST"]
PORT = os.environ["PORT"]
LOG = os.environ["EMAIL_LOG"]

# Configure logging
logger = logging.getLogger("flask_logger")
hdlr = logging.FileHandler(LOG)
formatter = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)


app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024
app.secret_key = os.environ["flask_key"]


@app.route("/", methods=["GET"])
def index():
    """
    Renders the index page.
    """
    return render_template("index.html")


@app.route("/", methods=["POST", "GET"])
def send_email_with_image():
    """
    Sends an email with an image attachment from the frontend..
    """
    if request.method == "POST":
        try:
            email = request.form.get("email")
            subject = request.form.get("subject")
            body = request.form.get("body")
            image_path = request.form.get("file")

            if email[-10:] != "bfi.org.uk":
                logger.error("Invalid email: %s, please enter valid email!", email)
                raise ValueError("Invalid email, please enter valid email!")

            if image_path is None:
                logger.error("User did not add an image path at all!")
                raise ValueError("Filepath is required")

            if not os.path.exists(image_path):
                logger.error(
                    "Invalid path: filepath provided does not exist -> %s", image_path
                )
                raise ValueError(
                    "Invalid path: Please check if the filepath does exist."
                )

            if "bp_nas" not in image_path:
                # app.logger.critical()
                logger.error("User has added a path to an images that not in bp_nas")
                raise ValueError("Image cant be found please move image to bp nas.")

            utils.send_email(email, subject, body, image_path)

            logger.info(
                "Email successfully sent to %s with subject $s", (email, subject)
            )
            flash(
                f"Email successfully sent to {email} with subject {subject}", "success"
            )

        except Exception as e:
            logger.critical("Email not sent due to reason: %s", str(e))
            flash(f"Email not sent due to reason: {str(e)}", "error")

            return render_template("error_page.html")
    return render_template("index.html")


if __name__ == "__main__":
    app.run(host=HOST, debug=False, port=PORT, use_reloader=False)
