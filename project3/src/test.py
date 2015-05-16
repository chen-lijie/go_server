#!/bin/python
import urllib2
url

def insert(key, value):
	para = urllib.urlencode({'key': key, 'value': value})
	f = urllib2.urlopen(url + 'insert', para)
	return f.read()

def countkey():
	f = urllib2.urlopen(url + 'countkey')
	return f.read()

insert('clj', 'fhq')
print countkey()