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


def change_point(data, num_critical_point):
	'''
	returns the critical points of the data 
	using the changepoint library in R.
	'''
	y = [x[1] for x in data]
	res = robjects.IntVector(y)

	robjects.r('''f <- function(x){
  				library("changepoint")
  				x.binseg <- cpt.mean(x, method="BinSeg", Q=3)
  				cpts(x.binseg)}''')

	r_f = robjects.globalenv['f']
	res = r_f(res)
	result = []
	for idx in list(res):
		result.append(data[int(idx)])
	return [x[0] for x in result]


def plot():
	link = 'http://ec2-52-0-219-222.compute-1.amazonaws.com/google'

	response = requests.get(link)

	content = response.json()
	content = sorted(content.items(), key=lambda x: datetime.strptime(x[0], '%Y-%m'))
	print change_point(content, 3)

	x_axis = [x[0] for x in content]
	y_axis = [x[1] for x in content]

	data = Data([
	    Scatter(
	        x=x_axis,
	        y=y_axis
	    )
	])

	plot_url = py.plot(data, filename='google_axes')

if __name__ == '__main__':
	plot()
