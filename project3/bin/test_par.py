#!/usr/bin/python
import sys
import urllib
import urllib2
import json
import os
import time
import threading

from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool

conf = json.loads(open('conf/settings.conf').read())

url = 'http://' + conf['primary'] + ':' + conf['port']

inserttime = []
gettime = []
totalinsert = 0
successinsert = 0

mutex = threading.Lock()

def insert(key, value):
	global totalinsert
	global successinsert
	para = urllib.urlencode({'key': key, 'value': value})
	time1 = time.time()
	f = urllib2.urlopen(url + '/kv/insert', para)
	time2 = time.time()
	inserttime.append(time2 - time1)
	re = f.read()
	mutex.acquire()
	totalinsert = totalinsert + 1
	if re.find('success') != -1:
		successinsert = successinsert + 1
	mutex.release()
	return re

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

def clear():
	data = json.loads(dump())
	for i in data:
		delete(i[0])
	
def insertAndGet(key):
	global success
	insert(str(key),str(key))
	re=get(key)
	if re.find('true') == -1:
		success = False;

os.system('bin/stopserver -p')
os.system('bin/stopserver -b')
os.system('bin/startserver -b')
os.system('bin/startserver -p')

success = True

pool = ThreadPool(20)
pool.map(insertAndGet,range(4000))

#shutdown the primary
os.system('bin/stopserver -p')
#restart the primary, it should still hold the data
os.system('bin/startserver -p')
for i in range(4000):
	re=get(i)
	if re.find('true') == -1:
		success = False;

#shutdown and restart every thing
os.system('bin/stopserver -p')
os.system('bin/stopserver -b')
os.system('bin/startserver -b')
os.system('bin/startserver -p')

#now, it should lost all the data!
for i in range(4000):
	re=get(i)
	if re.find('true') != -1:
		success = False;

data = json.loads(dump())
update('clj', 'FHQ')
insert('clj', 'fhq')
insert('', ' ')
dump()
update('clj', 'fHQ')
insert('fhq', 'clj')
insert('&&', 'clj')
clear()
insert('\xce\xd2\xe6\x88\x91', '\x62\x11')
insert('\xce\xd3\xe6\x88\x91', '\x62\x11')
insert('A', '~')
insert(' !@#$%^&*()', '\x00\x01\x02\x03\x04')
dump()

if success:
	print 'Result: success'
else:
	print 'Result: fail'
print >>sys.stderr, 'Insertion', '%d/%d' % (successinsert, totalinsert) 
print >>sys.stderr, 'Average latency:', "%f/%f" % (sum(inserttime) / len(inserttime), sum(gettime) / len(gettime))

inserttime = sorted(inserttime)
gettime = sorted(gettime)
print >>sys.stderr, 'Percentile latency:'
for i in range(10):
	print >>sys.stderr, inserttime[len(inserttime) * i / 10], gettime[len(gettime) * i / 10]
os.system('bin/stopserver -p')
os.system('bin/stopserver -b')
