__author__ = 'sogolmoshtaghi'

import Tokenize
import psycopg2
import matplotlib.pyplot as plt

def find_trend(c, query):
    '''
    Queries the trend of a search term by month.
    '''
    token_query = Tokenize.clean_tokenize(query)
    print token_query

    for word in token_query:
        c.execute('''SELECT id FROM words WHERE word='%s';'''%word)
        word_id = int(c.fetchone()[0])
        c.execute('''SELECT word_id, date, COUNT(date)
                    FROM trend WHERE word_id=%d GROUP BY word_id, date'''%word_id)
        word_by_months = c.fetchall()

        #TODO get rid of the matplotlib pieces.
        # word_by_months -> [(word_id, date, count), ...] 
        X_labels = [entry[1] for entry in word_by_months]   # ie. '2010-9'
        print X_labels
        Y = [entry[2] for entry in word_by_months] # ie. count of docs
        x = xrange(1, len(Y)+1)
        plt.plot(x, Y)
        plt.ylim(0, 1000)
        plt.xticks(x, X_labels)
        plt.xlabel('month')
        plt.ylabel('url count')
        plt.title(word, fontsize=21)
        plt.show()


if __name__ == '__main__':
    query = 'big data, Machine Learning, Google.'
    conn = psycopg2.connect(dbname='articles', user='postgres', password='', host='localhost')
    c = conn.cursor()
    find_trend(c, query)