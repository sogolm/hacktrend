from flask import Flask, jsonify, request
import flask
import pandas as pd
import psycopg2
import json
import collections
app = Flask(__name__)


def get_monthly_article_counts(data):
    months = [
    '2010-6',
    '2010-7',
    '2010-8',
    '2010-9',
    '2010-10',
    '2010-11',
    '2010-12',
    '2011-1',
    '2011-2',
    '2011-3',
    '2011-4',
    '2011-5',
    '2011-6',
    '2011-7',
    '2011-8',
    '2011-9',
    '2011-10',
    '2011-11',
    '2011-12',
    '2012-1',
    '2012-2',
    '2012-3',
    '2012-4',
    '2012-5',
    '2012-6',
    '2012-7',
    '2012-8',
    '2012-9',
    '2012-10',
    '2012-11',
    '2012-12',
    '2013-1',
    '2013-2',
    '2013-3',
    '2013-4',
    '2013-5',
    '2013-6',
    '2013-7',
    '2013-8',
    '2013-9',
    '2013-10',
    '2013-11',
    '2013-12',
    '2014-1',
    '2014-2',
    '2014-3',
    '2014-4',
    '2014-5',
    '2014-6',
    '2014-7',
    '2014-8',
    '2014-9',
    '2014-10',
    '2014-11',
    '2014-12',
    ]

    # Convert to a Pandas time series
    dates = map(lambda x: x[0], data)
    datetimes = pd.to_datetime(dates, unit="s")

    # Create the dataframe, indexing by dates
    df = pd.DataFrame(index=datetimes)
    # TODO: Encode the month in the tuple so that D3 can parse it
    counts = collections.OrderedDict()
    for month in months:
        counts[month] = len(df[month])

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

    We can then use D3 to plot the two arrays against each other.
    """

    # Postgres initialization
    conn = psycopg2.connect(database='articles', user='postgres', password='', host='localhost')
    cur = conn.cursor()

    cur.execute("""SELECT id FROM words WHERE word='%s';""" % term)
    word_id = int(cur.fetchone()[0])

    cur.execute("""SELECT article_date FROM trends WHERE word_id=%d;""" % word_id)
    rows = cur.fetchall()

    counts = get_monthly_article_counts(rows)
    return counts


@app.route("/<term>")
def index(term):
    return jsonify(**query(term))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True)

