import psycopg2
import mongo_to_sql

def create_inital_tables(conn, cur):
    """
    Creates sql tables for search and trend purposes.
    Tables: words, urls, wordloc.
    """

    cur.execute('''DROP TABLE IF EXISTS urls;''')
    cur.execute('''DROP TABLE IF EXISTS words;''')
    cur.execute('''DROP TABLE IF EXISTS wordloc;''')
    conn.commit()

    print "Creating tables ..."
    cur.execute('''CREATE TABLE urls(
                   id   INTEGER PRIMARY KEY,
                   date VARCHAR(100),
                   url  TEXT);''')

    cur.execute('''CREATE TABLE words(
                   id    INTEGER,
                   word  VARCHAR(100),
                   PRIMARY KEY(id)
                   );''')

    cur.execute('''CREATE TABLE wordloc(
                   id        INTEGER,
                   url_id    INTEGER,
                   word_id   INTEGER,
                   PRIMARY KEY(id)
                   );''')

    conn.commit()

    print 'Successfully created tables in potsgres'

def create_indices(con, cur):
  '''
  Creates indices for tables words (on word) 
  and wordloc (on word_id & url_id)
  '''
    cur.execute('''CREATE INDEX word_id ON words(word);''')
    cur.execute('''CREATE INDEX wordloc_word_id ON wordloc(word_id);''')
    cur.execute('''CREATE INDEX wordloc_url_id ON wordloc(url_id);''')
    conn.commit()


def create_other_tables(conn, cur):
    """
    Creates a bag of words, term frequency, tfidf 
    and trend table based on tables
    words, wordloc and urls
    """
    cur.execute('''DROP TABLE IF EXISTS bag;''')
    cur.execute('''DROP TABLE IF EXISTS max;''')
    cur.execute('''DROP TABLE IF EXISTS tf_inter;''')
    cur.execute('''DROP TABLE IF EXISTS tfreq;''')
    cur.execute('''DROP TABLE IF EXISTS idf;''')
    cur.execute('''DROP TABLE IF EXISTS tfidf;''')
    cur.execute('''DROP TABLE IF EXISTS trend;''')
    conn.commit()

    cur.execute('''CREATE TABLE bag AS
                  SELECT urls.id as url_id, words.id AS word_id, COUNT(1) as cnt
                  FROM wordloc
                  JOIN urls
                  ON urls.id=wordloc.url_id
                  JOIN words
                  ON words.id=wordloc.word_id
                  GROUP BY urls.id, words.id);''')

    cur.execute('''CREATE TABLE max(
                  CREATE TABLE max AS
                  SELECT url_id, MAX(cnt) 
                  FROM bag GROUP BY url_id ORDER BY url_id
                   );''')

    cur.execute('''CREATE TABLE tf_inter(
                  CREATE TABLE tf_inter AS
                  SELECT b.url_id, b.word_id, b.cnt, m.max
                  FROM bag AS b
                  JOIN max AS m
                  ON b.url_id = m.url_id
                  ORDER BY b.url_id
                   );''')

    cur.execute('''CREATE TABLE tfreq(
                   id        INTEGER,
                   url_id    INTEGER,
                   word_id   INTEGER,
                   PRIMARY KEY(id)
                   );''')

    cur.execute('''CREATE TABLE idf(
                  SELECT bag.word_id, 
                  CAST(LOG(121641./COUNT(wu.url_count)) AS FLOAT) AS idf_score
                  FROM bag
                  JOIN word_url_cnt as wu
                  ON bag.word_id=wu.word_id
                  GROUP BY bag.word_id ORDER BY bag.word_id
                   );''')

    cur.execute('''CREATE TABLE tfidf(
                    SELECT tfreq.url_id AS url_id, tfreq.word_id AS word_id,
                    CAST(tfreq.tf * idf.idf_score AS FLOAT) AS tfidf
                    FROM tfreq JOIN idf 
                    ON tfreq.word_id=idf.word_id
                    ORDER BY url_id, tfreq
                   );''')

    cur.execute('''CREATE TABLE trend(
                    SELECT words.id AS word_id, urls.id as url_id, SUBSTRING(urls.date, 0, 8) AS date
                    FROM wordloc
                    JOIN urls
                    ON urls.id=wordloc.url_id
                    JOIN words
                    ON words.id=wordloc.word_id
                    GROUP BY words.id, urls.id ORDER BY words.id, date
                   );''')


if __name__ == '__main__':
    conn = psycopg2.connect(dbname='articles', user='sogolmoshtaghi', host='localhost')
    cur = conn.cursor()
    create_inital_tables(conn, cur)
    mongo_to_sql.main()
    create_indices(con, cur)
    create_other_tables(conn, cur)
