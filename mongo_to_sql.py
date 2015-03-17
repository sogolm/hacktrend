from pymongo import MongoClient
from boilerpipe.extract import Extractor
import psycopg2
import time
import Tokenize


class transfer_data():

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

    def extract_content(self, link):
        """
        Extracts the main article from link using
        boilerpipe to be fed into clean_tokenize.
        """
        try:
            extractor = Extractor(extractor='ArticleExtractor', url=link)
            content = extractor.getText()
        except:
            content = ''

        return content

    def execute_q(self, query):
        """
        Wraps sql insertions in try, excepts
        to deal with unexpected data insertions.
        """
        try:
            self.cur.execute(query)
        except Exception as err_msg:
            print err_msg
            self.conn.rollback()


    def convert_time(self, epoch_date):
        return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(epoch_date))

    def data_pipeline(self):
        """
        For each post in mongodb, cleans and tokenizes the content and
        inserts into data into pre-created postgres tables words, wordloc
        and urls.
        """
        arts_with_content = self.table.find({'status_code': 200}, timeout=False)

        wordloc_id = 0
        worddict = {}
        urlid = 0
        for art_i, article in enumerate(arts_with_content):
            # Commit to database after every 2000 posts 
            if art_i == 0 or art_i == 1 or art_i % 2000 == 0:
                self.conn.commit()
            if len(article['link_content']) > 30:
                content = self.extract_content(article['link'])
                clean_content_lst = Tokenize.clean_tokenize(content)
                clean_content = " ".join(clean_content_lst)
                self.table.update({"_id": article["_id"]}, {"$set": {"clean_content": clean_content}})
                article['clean_content'] = clean_content

                if len(article['clean_content']) > 0:
                    date_str = self.convert_time(article["date"])
                    self.execute_q("INSERT INTO urls VALUES (%d, '%s', '%s')" % (urlid, date_str, article['link']))

                    for i, word in enumerate(article['clean_content'].split()):

                        # Only insert if the word is less than 100 characters.
                        if len(word) < 100:
                            if word not in worddict:
                                wordid = len(worddict)
                                worddict[word] = wordid

                                self.execute_q("INSERT INTO words VALUES (%d, '%s');" % (wordid, word))
                            else:
                                wordid = worddict[word]

                            self.execute_q("INSERT INTO wordloc VALUES (%d, %d, %d);" % (wordloc_id, urlid, wordid))
                            wordloc_id += 1
                        else:
                            print "Long word:", word
                    urlid += 1

        self.conn.commit()
        self.cur.close()
        self.conn.close()


def main():
    client = MongoClient()
    transformer = transfer_data(client, 'test', 'hackerfulldata')
    transformer.data_pipeline()  