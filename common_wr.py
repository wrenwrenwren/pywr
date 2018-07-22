import pandas as pd
import dask.dataframe as dd
import bcolz 
import urllib
import os 
import glob
import multiprocessing

def getbitmextradedata(start,end):
	
	dates = pd.date_range(start, end, freq='D')
	dates = dates.strftime('%Y%m%d').tolist()
	urldownloader = urllib.URLopener()
	
	for i in dates:
		tmp_name = i + '.csv.gz'
		url = 'https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/trade/'
		url = url + tmp_name
		print(tmp_name)

		try:
			urldownloader.retrieve(url, tmp_name)
		except:
			pass

def getbitmexquotedata(start,end):
	
	dates = pd.date_range(start, end, freq='D')
	dates = dates.strftime('%Y%m%d').tolist()
	urldownloader = urllib.URLopener()
	
	for i in dates:
		tmp_name = i + '.csv.gz'
		url = 'https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/quote/'
		url = url + tmp_name
		print(tmp_name)

		try:
			urldownloader.retrieve(url, tmp_name)
		except:
			pass

def processbitmexdata(files, type):

	df = dd.read_csv(files, compression='gzip')
	symbols = df.symbol.drop_duplicates().compute()
    
    if type = 'trade':
    	extension = 'trade.bcolz'
    else:
    	extension = 'quote.bcolz'

	for i in symbols:
		tmp = df[df.symbol == i].compute()
		filename = i + '.bcolz'
		ct = bcolz.ctable.fromdataframe(tmp, rootdir=filename)

	p = multiprocessing.Pool(4)
    p.map(os.remove, glob.glob("*.csv.gz"))








