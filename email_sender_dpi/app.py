from flask import Flask, render_template, request, jsonify, redirect, url_for, flash
import utils

app = Flask(__name__, template_folder='templates')

app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16 MB max file size
app.secret_key = "pass"

@app.route('/', methods=['GET'])
def index():
    # Return the HTML template for GET requests
    return render_template("index.html")

@app.route('/', methods=['POST', 'GET'])
def send_screenshot():
    if request.method == 'POST':
        try:
            email = request.form.get('email')
            subject = request.form.get('subject')
            body = request.form.get('body')
            image_path = request.form.get('file')
            
            if email[-10:] != 'bfi.org.uk':
                 raise ValueError("Invalid email, please enter valid email!")
            # if not email:
            #     raise ValueError("Email address is required")

            if not image_path:
                 raise ValueError("filepath is required")

            utils.send_email(email, subject, body, image_path)

            flash(f'Email successfully sent to {email}', 'success')

            return redirect(url_for('send_screenshot'))
        
        except Exception as e:
                # Flash error message
                flash(f'Email not sent due to reason: {str(e)}', 'error')
                
                # Redirect to refresh the page
                return redirect(url_for('send_screenshot'))
    return render_template("index.html")
    




if __name__ == "__main__":
    app.run(debug=True)
