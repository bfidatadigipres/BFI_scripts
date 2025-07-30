import logging
import sys
import os
from flask import Flask, flash, render_template, request

sys.path.append(os.environ["CODE"])
import utils

app = Flask(__name__, template_folder="templates")

logger = logging.getLogger("flask_logger")
hdlr = logging.FileHandler(
    "/mnt/bp_nas/admin/automation_logs/Logs/email_sender_dpi.log"
)
formatter = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)


app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024
app.secret_key = os.environ["flask_key"]


@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")


@app.route("/", methods=["POST", "GET"])
def send_screenshot():
    if request.method == "POST":
        try:
            email = request.form.get("email")
            subject = request.form.get("subject")
            body = request.form.get("body")
            image_path = request.form.get("file")

            if email[-10:] != "bfi.org.uk":
                logger.error(f"Invalid email: {email}, please enter valid email!")
                raise ValueError("Invalid email, please enter valid email!")

            if image_path is None:
                logger.error("User did not add an image path at all!")
                raise ValueError("Filepath is required")

            if not os.path.exists(image_path):
                logger.error(
                    f"Invalid path: filepath provided does not exist -> {image_path}"
                )
                raise ValueError(
                    "Invalid path: Please check if the filepath does exist."
                )

            if "bp_nas" not in image_path:
                # app.logger.critical()
                logger.error("User has added a path to an images that not in bp_nas")
                raise ValueError("Image cant be found please move image to bp nas.")

            utils.send_email(email, subject, body, image_path)

            logger.info(f"Email successfully sent to {email}")
            flash(
                f"Email successfully sent to {email} with subject {subject}", "success"
            )

        except Exception as e:
            logger.critical(f"Email not sent due to reason: {str(e)}")
            flash(f"Email not sent due to reason: {str(e)}", "error")

            return render_template("error_page.html")
    return render_template("index.html")


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True, port=8000)
