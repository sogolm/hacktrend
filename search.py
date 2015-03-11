import Tokenize
import psycopg2
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer
from scipy.sparse import lil_matrix
from sklearn.externals import joblib
from sklearn.metrics.pairwise import linear_kernel


_kmeans_model = None
_feature_mat_model = None
_kmeans_file = 'kmeans.pkl'
_feature_mat_file = 'featurematrix.pkl'


def get_kmeans_model():
    global _kmeans_model
    if _kmeans_model is None:
        with open(_kmeans_file, 'r') as f:
            _kmeans_model = joblib.load(_kmeans_file)
    return _kmeans_model


def get_feature_mat_model():
    global _feature_mat_model
    if _feature_mat_model is None:
        with open(_feature_mat_file, 'r') as f:
            _feature_mat_model = joblib.load(_feature_mat_file)
    return _feature_mat_model


def calculate_tfidf(query):
    """
    Calculates the tfidf of the user input query
    based on the corpus.
    """
    count_vect = CountVectorizer(tokenizer=Tokenize.clean_tokenize)
    query_count_vect = count_vect.fit_transform([query]).toarray()
    vocabs = count_vect.vocabulary_

    c.execute('''SELECT COUNT(*) FROM urls;''')
    doc_count = int(c.fetchone()[0])

    tfidf = {}

    for word, idx in vocabs.iteritems():
        c.execute(''' SELECT id FROM words
                      WHERE word='%s';''' %word)
        word_id = int(c.fetchone()[0])

        c.execute('''SELECT url_count FROM word_url_cnt
                     WHERE word_id=%d;'''%word_id)
        url_cnt = int(c.fetchone()[0])

        idf = np.log(float(doc_count)/url_cnt)
        tfidf[word_id] = idf * query_count_vect[0][idx]

    return tfidf


def construct_feature_vector(tfidf_dict):
    """
    Constructs the feature vector representation
    of user input based on its tfidf
    """
    c.execute('''SELECT COUNT(*) FROM words;''')
    word_count = int(c.fetchone()[0])

    query_vect = lil_matrix((1, word_count), dtype=np.float_)

    for word_id, tfidf in tfidf_dict.iteritems():
        query_vect[0, word_id] = tfidf
    return query_vect


def classify(q_vect):
    """
    Classifies the user input into a cluster
    using a pre-trained model on the corpus
    """
    get_kmeans_model()
    cluster = _kmeans_model.predict(q_vect)
    return cluster


def compute_cosine_sim(q_vect, cluster):
    """
    Computes the cosine similarity between
    user input and every other article in
    cluster and returns the top results.
    """
    get_feature_mat_model()
    c.execute('''SELECT url_id FROM url_label
                 WHERE label=%d'''%cluster)

    print "querying the urls from labels"
    urls = c.fetchall()  # returns a list of tuples
    urls = np.array(urls)[:, 0]
    print "number of urls: %d" %len(urls)
    print _feature_mat_model.shape
    #urls_in_clusters = _feature_mat_model[urls] #TODO: commenting out for now. too slow!
    indices = np.arange(len(urls))
    np.random.shuffle(indices)
    urls_in_clusters = _feature_mat_model[indices[:100]]
    print urls_in_clusters.shape
    print "got urls in cluster"
    cosine_similarities = linear_kernel(q_vect, urls_in_clusters).flatten()
    print "got cosine similarity"
    sorted_url_ids = cosine_similarities.argsort()[:-5:-1]  # TODO: for now just get the top 5. fix this later
    print "sorted urls"
    return sorted_url_ids


def get_top_results(sorted_urls_ids):
    print "getting top results from ranked urls by ids"
    id_str = ', '.join(map(str, sorted_url_ids))
    print id_str
    c.execute('''SELECT url FROM urls WHERE id IN (%s);'''%id_str)
    results = c.fetchall()
    results = np.array(results).flatten()
    return results


if __name__ == '__main__':
    query = 'big data machine learning at Google.'
    conn = psycopg2.connect(dbname='articles', user='postgres', password='', host='localhost')
    c = conn.cursor()
    tfidf_dict = calculate_tfidf(query)
    query_vector = construct_feature_vector(tfidf_dict)
    cluster = classify(query_vector)
    sorted_url_ids = compute_cosine_sim(query_vector, cluster)
    results = get_top_results(sorted_url_ids)
    print results
    c.close()
    conn.close()


