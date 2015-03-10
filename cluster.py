import psycopg2
import numpy as np 
from sklearn.cluster import KMeans 
from scipy.sparse import lil_matrix
from sklearn.externals import joblib


def create_feature_matrix(c):
	"""
	Creates a sparse matrix representing each 
	document using the TFIDF based on sql tables.
	This feature matrix is fed into the kmeans 
	to cluster the documents.
	"""
	c.execute('''SELECT COUNT(*) FROM urls;''')
	url_count = int(c.fetchone()[0])

	c.execute('''SELECT COUNT(*) FROM words;''')
	word_count = int(c.fetchone()[0])

	# instantiate a sparse matrix of size (url_count X word_count)
	mat = lil_matrix((url_count, word_count), dtype=np.float_)

	c.execute('''SELECT * FROM tfidf;''')
	doc = c.fetchall()

	for row in doc:
		mat[row[0], row[1]] = row[2]

	return mat

def cluster(feature_matrix, c):
	"""
	Clusters documents (represented by feature_matrix).
	Creates a postgres table to keep track of the documents
	labels.
	"""
	#TODO: change the cluster number once the full data is in. 
	kmeans = KMeans(n_clusters=15)
	kmeans.fit(feature_matrix)
	centers = kmeans.cluster_centers_
	sort_args = np.argsort(centers)[:, -1:-11:-1]
	for i, row in enumerate(sort_args):
		print "topic %d: " %i
		for idx in row:
			c.execute('''SELECT word FROM words_sub
								WHERE id=%d;
								''' %idx)
			word = str(c.fetchone()[0])
			print word
		print "\n"

	# Create a table of url_id and cluster num

	c.execute('''DROP TABLE IF EXISTS url_label_sub;''')
	c.execute(''' CREATE TABLE url_label_sub (
				  label  INTEGER,
				  url_id INTEGER);''')

	print "created table url_label_sub"

	labels = kmeans.labels_
	print labels.shape #should be the size of urls (each url should have a label)
	for i, label in enumerate(labels):
		c.execute("INSERT INTO url_label_sub VALUES (%d, %d);" % (label, i))

	print "Populated table url_label_sub"
	return kmeans

if __name__ == '__main__':
	conn = psycopg2.connect(dbname='articles', user='postgres', password='', host='localhost')
	c = conn.cursor()
	feature_matrix = create_feature_matrix(c)
	joblib.dump(feature_matrix, 'featurematrix.pkl')
	print len(feature_matrix)
	# kmeans = cluster(feature_matrix, c)
	# joblib.dump(kmeans, 'kmeans.pkl')
	# conn.commit()
	c.close()
	conn.close()