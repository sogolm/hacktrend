import plotly.plotly as py
import plotly.tools as tls
tls.set_credentials_file(username='sogolm', api_key='k5cjm5j1p4')
from plotly.graph_objs import *
from datetime import datetime
import json
import requests
from collections import OrderedDict
import rpy2.robjects as robjects
r=robjects.r
import psycopg2
import datetime
from pprint import pprint


def to_datetime(date_string):
    '''
    Converts date strings with 
    format YYYY-MM to datetime object 
    '''
    return datetime.datetime.strptime(date_string, '%Y-%m')


def to_epoch_date(date):
    '''
    Converts datetime object to epoch date integer
    '''
    return int((date - datetime.datetime(1970, 1, 1)).total_seconds())


def to_date_string(date):
    return datetime.datetime.strftime(date, "%Y-%m")


def get_change_points(data):
    '''
    returns the critical points of the data
    using the changepoint library in R.
    '''
    y = [x[1] for x in data]
    res = robjects.IntVector(y)

    robjects.r('''f <- function(x){
                library("changepoint")
                x.binseg <- cpt.mean(x, method="BinSeg", Q=4)
                cpts(x.binseg)}''')

    r_f = robjects.globalenv['f']
    res = r_f(res)
    result = []
    for idx in list(res):
        result.append(data[int(idx)])
    return [x[0] for x in result]


def get_top_articles_for_months(term, months):
    '''
    For the critical months, queries for the top 10 urls
    that contain the term ordered by TFIDF. 
    '''
    # Convert the term to lowercase, strip whitespace
    #and use only the first word in multi-word queries
    term = term.lower().strip().split(" ")[0]

    # Postgres initialization
    conn = psycopg2.connect(database='articles', user='postgres', password='', host='localhost')
    cur = conn.cursor()
    cur.execute("""SELECT * FROM words WHERE word='%s'""" % term)
    word_id = int(cur.fetchone()[0])

    results = {}
    for month in months:
        date_range_start = to_epoch_date(month)
        date_range_end = to_epoch_date(datetime.datetime(month.year, month.month+1, month.day))
        cur.execute("""SELECT * FROM trends
            WHERE word_id=%d AND article_date > %d AND article_date < %d
            ORDER BY article_tfidf DESC LIMIT 10"""
            % (word_id, date_range_start, date_range_end))
        articles = cur.fetchall()
        article_ids = [str(article[1]) for article in articles]
        articles_in_clause = "(" + ",".join(article_ids) + ")"
        cur.execute('''SELECT * FROM urls where id in %s''' % articles_in_clause)
        urls = cur.fetchall()
        results[to_date_string(month)] = urls

    return results


def plot_query_term(term, content):
    '''
    Drives plotting the time series of the searched term based
    on content -- dictionary of dates mapped to counts --, extracting
    critical dates for the term and returning top results
    based on TFIDF
    '''
    content = sorted(content.items(), key=lambda x: datetime.datetime.strptime(x[0], '%Y-%m'))
    dates = map(lambda x: (to_datetime(x[0]), x[1]), content)

    x_axis = [x[0] for x in content]
    y_axis = [x[1] for x in content]

    data = Data([
        Scatter(
            x=x_axis,
            y=y_axis
        )
    ])

    change_points = get_change_points(dates)
    articles = get_top_articles_for_months(term, change_points)
    py.plot(data, filename='google_axes')
    return articles

if __name__ == '__main__':
    plot()

