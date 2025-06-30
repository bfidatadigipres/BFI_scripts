import logging

from flask import Flask, flash, render_template, request

import utils

app = Flask(__name__, template_folder="templates")

logger = logging.getLogger("flask_logger")
hdlr = logging.FileHandler("/Users/mohameds/email_temp/app.log")
formatter = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)


app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 16 MB max file size
app.secret_key = "*********"


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
                logger.error("Invalid email, please enter valid email!")
                raise ValueError("Invalid email, please enter valid email!")

            if not image_path:
                logger.error("User did not add an image at all!")
                raise ValueError("filepath is required")

            if "bp_nas" not in image_path:
                # app.logger.critical()
                logger.error("User has added a path to an images that not in bp_nas")
                raise ValueError("Image cant be found please move image to bp nas.")

            utils.send_email(email, subject, body, image_path)

            logger.info(f"Email successfully sent to {email}", "success")
            flash(
                f"Email successfully sent to {email} with subject {subject}", "success"
            )

        except Exception as e:
            logger.critical(f"Email not sent due to reason: {str(e)}")
            flash(f"Email not sent due to reason: {str(e)}", "error")

            return render_template("error_page.html")
    return render_template("index.html")


if __name__ == "__main__":
    app.run(debug=True)
