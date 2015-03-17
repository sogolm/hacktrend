from flask import Flask, jsonify, request
import pandas as pd
import psycopg2
import json
import collections
from datetime import datetime
from flask import render_template
from flask.ext.cors import CORS
from plot import plot_query_term
from months import get_months
app = Flask(__name__)
CORS(app)


def get_monthly_article_counts(data):
    '''
    Returns a dictionary of each month mapped to the
    number of articles in that month for the time series.
    '''
    months = get_months()

    # Convert to a Pandas time series
    dates = map(lambda x: x[0], data)
    datetimes = pd.to_datetime(dates, unit="s")

    # Create the dataframe, indexing by dates
    df = pd.DataFrame(index=datetimes)
    counts = collections.OrderedDict()
    for month in months:
        try:
            counts[month] = len(df[month])
        except:
            pass

    return counts

def query(term):
    """
    Queries the database for results for a given search term, and returns
    them as two arrays in a JSON object. The two arrays should have the same
    length.

    results: {
        "dates": []
        "counts": []
    }
    """

    # Convert the term to lowercase, strip whitespace, and use only the first word
    # in multi-word queries.
    app.logger.info(term)
    term = term.lower().strip().split(" ")[0]

    conn = psycopg2.connect(database='articles', user='postgres', password='', host='localhost')
    cur = conn.cursor()
    cur.execute("""SELECT id FROM words WHERE word='%s';""" % term)

    word_row = cur.fetchone()
    if word_row is None:
        return collections.OrderedDict()

    word_id = int(word_row[0])
    cur.execute("""SELECT article_date FROM trends WHERE word_id=%d;""" % word_id)
    rows = cur.fetchall()

    counts = get_monthly_article_counts(rows)
    return counts


@app.route('/')
@app.route('/home')
def home():
    """Renders the home page."""
    return render_template(
        'index.html',
        title='Hacker Trend',
        year=datetime.now().year,
    )


@app.route('/contact')
def contact():
    """Renders the contact page."""
    return render_template(
        'contact.html',
        title='Contact',
        year=datetime.now().year,
        message='Your contact page.'
    )


@app.route('/about')
def about():
    """Renders the about page."""
    return render_template(
        'about.html',
        title='About',
        year=datetime.now().year,
        message='Your application description page.'
    )


@app.route("/api/plot/<term>")
def api_plot(term):
    counts = query(term)
    app.logger.info(len(counts))
    if len(counts.items()) < 3:
        app.logger.info("error")
        return jsonify({
            "error": "Word not found"
            })
    return jsonify(**plot_query_term(term, counts))


@app.route("/api/monthly_counts/<term>")
def api_monthly_counts(term):
    return jsonify(**query(term))


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True)

