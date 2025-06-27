import logging

from flask import Flask, flash, redirect, render_template, request, url_for

import utils

app = Flask(__name__, template_folder="templates")

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)-8s %(name)-15s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 16 MB max file size
app.secret_key = "********"


@app.route("/", methods=["GET"])
def index():
    """
    returns the rendered page of index.html
    """
    return render_template("index.html")


@app.route("/", methods=["POST", "GET"])
def send_screenshot():
    """
    process form and make the request to send the email
    with the information or send an error.
    """
    if request.method == "POST":
        try:
            email = request.form.get("email")
            subject = request.form.get("subject")
            body = request.form.get("body")
            image_path = request.form.get("file")

            if email[-10:] != "bfi.org.uk":
                logging.error("Invalid email, please enter valid email!")
                raise ValueError("Invalid email, please enter valid email!")

            if not image_path:
                logging.error("User did not add an image at all!")
                raise ValueError("filepath is required")

            if "bp_nas" not in image_path:
                # app.logger.critical()
                logging.error("User has added a path to an images that not in bp_nas")
                raise ValueError("Image cant be found please move image to bp nas.")

            utils.send_email(email, subject, body, image_path)

            logging.info(f"Email successfully sent to {email}", "success")
            flash(f"Email successfully sent to {email}", "success")

            return redirect(url_for("send_screenshot"))

        except Exception as e:
            logging.critical(f"Email not sent due to reason: {str(e)}")
            flash(f"Email not sent due to reason: {str(e)}", "error")

            return render_template("error_page.html")

    return render_template("index.html")


if __name__ == "__main__":
    app.run(debug=True)
