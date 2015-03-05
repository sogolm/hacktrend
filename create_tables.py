import psycopg2

conn = psycopg2.connect(dbname='articles', user='sogolmoshtaghi', host='localhost')
cur = conn.cursor()

def create_sqltables():
    """
    Creates sql tables for search and trend purposes.
    """

    cur.execute('''DROP TABLE IF EXISTS urls;''')
    cur.execute('''DROP TABLE IF EXISTS words;''')
    cur.execute('''DROP TABLE IF EXISTS wordloc;''')
    conn.commit()

    print "Creating tables ..."
    cur.execute('''CREATE TABLE urls(
                   id   INTEGER PRIMARY KEY,
                   url  TEXT);''')

    cur.execute('''CREATE TABLE words (
                   id    INTEGER,
                   word  VARCHAR(100),
                   PRIMARY KEY(id)
                   );''')

    cur.execute('''CREATE TABLE wordloc (
                   id        INTEGER,
                   url_id    INTEGER,
                   word_id   INTEGER,
                   PRIMARY KEY(id)
                   );''')

    conn.commit()
    conn.close()
    print 'Successfully created tables in potsgres'

if __name__ == '__main__':
    create_sqltables()
