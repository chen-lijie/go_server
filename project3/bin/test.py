#!/bin/python
import urllib
import urllib2
import json
import os
import time
conf = json.loads(open('../conf/settings.conf').read())

url = 'http://' + conf['primary'] + ':' + conf['port']

inserttime = []
gettime = []
def insert(key, value):
	para = urllib.urlencode({'key': key, 'value': value})
	time1 = time.time()
	f = urllib2.urlopen(url + '/kv/insert', para)
	time2 = time.time()
	inserttime.append(time2 - time1)
	return f.read()

def delete(key):
	para = urllib.urlencode({'key': key})
	f = urllib2.urlopen(url + '/kv/delete', para)
	return f.read()

def get(key):
	para = urllib.urlencode({'key': key})
	time1 = time.time()
	f = urllib2.urlopen(url + '/kv/get?' +  para)
	time2 = time.time()
	gettime.append(time2 - time1)
	return f.read()

def update(key, value):
	para = urllib.urlencode({'key': key, 'value': value})
	f = urllib2.urlopen(url + '/kv/update', para)
	return f.read()

def countkey():
	f = urllib2.urlopen(url + '/kvman/countkey')
	return f.read()

def dump():
	f = urllib2.urlopen(url + '/kvman/dump')
	return f.read()

def shutdown():
	f = urllib2.urlopen(url + '/kvman/shutdown')
	return f.read()


os.system('../bin/stopserver -p')
os.system('../bin/stopserver -b')
os.system('../bin/startserver -b')
os.system('../bin/startserver -p')

for i in conf:
	print insert(i, 'QWERTY')
data = json.loads(dump())
for i in data:
	print get(i[0]), countkey()
	delete(i[0])
print update('clj', 'FHQ')
print insert('clj', 'fhq')
print update('clj', 'fHQ')
print insert('fhq', 'clj')
print insert(' !@#$%^&*()', '\x00\x01\x02\0x03\x04')
print dump()
print sum(inserttime) / len(inserttime)
print sum(gettime) / len(gettime)
os.system('../bin/stopserver -p')
os.system('../bin/stopserver -b')
