from pymongo import MongoClient
from string import punctuation
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from pymongo import MongoClient
from boilerpipe.extract import Extractor
from nltk.stem.wordnet import WordNetLemmatizer
import psycopg2
import string
import re
from nltk.stem import WordNetLemmatizer, SnowballStemmer
import sys
import time
import pymongo
import sys
import logging
from psycopg2 import IntegrityError
import pickle as pkl

class build_TF_IDF():

    # TODO: should these be class attributes?
    urls = 'docs'
    words = 'words'
    wordloc = 'wordlocation'

    def __init__(self, client, database, table):
        self.client = client
        self.db = client[database]
        self.table = self.db[table]
        conn_cur = self.create_connection()
        self.cur, self.conn = conn_cur[0], conn_cur[1]

    def create_connection(self):
        """
        Connects to db 'articles'
        (creates if it doesn't exist')
        and returns the cursor
        """

        conn = psycopg2.connect(dbname='articles', user='sogolmoshtaghi', host='localhost')
        cur = conn.cursor()
        print 'Successfully connected to postgres'
        return cur, conn


    def clean_tokenize(self, link):
        """
        returns the text from htmlDoc using boilerpipe and removes
        stopwords, symbols and punctuation
        """

        try:
            extractor = Extractor(extractor='ArticleExtractor', url=link)
            content = extractor.getText()
        except:
            content = ''

        # removing the URLs from content
        if len(content) > 0:
            content = content.lower()

            #removing links
            content = re.sub('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+',
                             '', content, flags=re.DOTALL)

            # removing numbers and newlines from content
            content = re.sub("\d+", " ", content)

            # TODO: remove more things via regex

            # tokenize
            word_list = word_tokenize(content)
            filtered = [w.encode('ascii', 'ignore').lower().replace('\u2605','')
                        for w in word_list if w.encode('ascii', 'ignore').lower().replace('\u2605','').replace('"','')
                        not in ALL_STOPWORDS]

            lmtzr = WordNetLemmatizer()
            lem_words = [lmtzr.lemmatize(w) for w in filtered]
            return ' '.join(lem_words)
        else:
            return ''

    def table_tfidf(self):
        '''
        Builds a TF-IDF vectorizer using clean articles in the table.
        '''

        pass

    # def create_sqltables(self):
    #     """
    #     Creates sql tables for search and trend purposes.
    #     """
    #
    #     self.cur.execute('''DROP TABLE IF EXISTS urls;''')
    #     self.cur.execute('''DROP TABLE IF EXISTS words;''')
    #     self.cur.execute('''DROP TABLE IF EXISTS wordloc;''')
    #     self.conn.commit()
    #
    #     print "Creating tables ..."
    #     self.cur.execute('''CREATE TABLE urls(
    #                id integer PRIMARY KEY,
    #                url text);''')
    #
    #     self.cur.execute('''CREATE TABLE words (
    #                id integer PRIMARY KEY,
    #                word text);''')
    #
    #     self.cur.execute('''CREATE TABLE wordloc (
    #                id integer,
    #                url_id integer,
    #                word_id integer);''')
    #
    #     self.conn.commit()
    #     print 'Successfully created tables in potsgres'

    def create_indices(self):
        self.cur.execute('''CREATE INDEX words_word ON words(word);''')
        self.cur.execute('''CREATE INDEX wordloc_wordid ON wordloc(word_id);''')
        self.cur.execute('''CREATE INDEX wordloc_urlid ON wordloc(url_id);''')

    def data_pipeline(self):
        """
        Runs the hackernews corpus through cleaning, tokenization
        and TF_IDF
        """
        # self.create_sqltables()
        arts_with_content = self.table.find({'status_code': 200})

        # half = self.table.count() / 2
        # if first_half == 'true':
        #     data = self.table.find({'link_content': {'$exists': True}}).sort('_id', pymongo.ASCENDING).limit(half)
        # else:
        #     data = self.table.find({'link_content': {'$exists': True}}).sort('_id', pymongo.ASCENDING).skip(half).limit(half)
        # # chunk = data[self.start: self.end]


        wordloc_id = 0
        worddict = {}
        urlid = 0
        for art_i, article in enumerate(arts_with_content):
            if art_i == 0 or art_i == 1 or art_i % 2000 == 0:
                print 'Done', art_i
                self.conn.commit()
            if len(article['link_content']) > 30:
                clean_content = self.clean_tokenize(article['link'])

                self.table.update({"_id": article["_id"]}, {"$set": {"clean_content": clean_content}})
                article['clean_content'] = clean_content

                if len(article['clean_content']) > 0:
                    self.cur.execute("INSERT INTO urls VALUES (%d, '%s')" % (urlid, article['link']))

                    for i, word in enumerate(article['clean_content'].split()):
                        word = word.replace("'", "''")
                        if word not in worddict:
                            wordid = len(worddict)
                            worddict[word] = wordid
                            self.cur.execute("INSERT INTO words VALUES (%d, '%s');" % (wordid, word))
                        else:
                            wordid = worddict[word]

                        self.cur.execute("INSERT INTO wordloc VALUES (%d, %d, %d);" % (wordloc_id, urlid, wordid))
                        wordloc_id += 1
                    urlid += 1
        self.conn.commit()

        self.create_indices()

        print "num words: ", len(worddict)
        print "num_urls: ", urlid
        self.conn.close()


if __name__ == '__main__':
    # start = int(sys.argv[1])
    # end = int(sys.argv[2])
    # first_half = sys.argv[1]
    # name =


    ADDITIONAL_STOPWORDS = ['said', 'would', 'like', 'many', 'also', 'could',
                            'mr', 'ms', 'mrs', 'may', 'even', 'say', 'much',
                            'going', 'might', 'dont', 'go', 'another', 'around',
                            'says', 'editor']
    ALL_STOPWORDS = set(stopwords.words('english') +
                        ADDITIONAL_STOPWORDS +
                        list(string.punctuation))
    client = MongoClient()

    # currently using the test db. change it to the main mongodb.
    builder = build_TF_IDF(client, 'test', 'hackerfulldata')
    builder.data_pipeline()

