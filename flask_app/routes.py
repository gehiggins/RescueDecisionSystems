from flask import Flask, render_template

# Create Flask app
app = Flask(__name__, static_folder='static', template_folder='templates')

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/sarsat-analysis')
def sarsat_analysis():
    return render_template('sarsat-analysis.html')

if __name__ == '__main__':
    app.run(debug=True)
