# coding=utf8
import os
import urllib2
import urllib
import random
import threading
import json
random.seed(100)

test={}
def randstr():
	l=random.randrange(1,8282)
	return ''.join([chr(random.randrange(1,127)) for i in xrange(l)])+'哈'
for i in xrange(10):
	test[randstr()]=randstr()

os.system('./stopserver -p')
os.system('./stopserver -b')
os.system('./startserver -p')
os.system('./startserver -b')

for k,v in test.iteritems():
	ret=json.loads(urllib2.urlopen('http://127.0.0.1:5644/kv/insert?'+urllib.urlencode({'key':k,'value':v})).read())
	if 'success' not in ret or ret['success']!="true":
		print 'insert fail'
for k,v in test.iteritems():
	ret=json.loads(urllib2.urlopen('http://127.0.0.1:5644/kv/get?'+urllib.urlencode({'key':k})).read())
	if 'success' not in ret or ret['success']!="true" or ret['value']!=v.decode('utf8'):
		print 'get fail',len(k),len(v),len(ret['value'])
os.system('./stopserver -b')
os.system('./startserver -b')
tasks=test.items()
for k,v in tasks[:2]:
	ret=json.loads(urllib2.urlopen('http://127.0.0.1:5644/kv/delete?'+urllib.urlencode({'key':k})).read())
	if 'success' not in ret or ret['success']!="true" or ret['value']!=v.decode('utf8'):
		print 'delete fail'
	del test[k]
for k,v in tasks[2:4]:
	nv=randstr()
	ret=json.loads(urllib2.urlopen('http://127.0.0.1:5644/kv/update?'+urllib.urlencode({'key':k,'value':nv})).read())
	test[k]=nv
	if 'success' not in ret or ret['success']!="true":
		print 'update fail'
for k,v in tasks[2:4]:
	ret=json.loads(urllib2.urlopen('http://127.0.0.1:5644/kv/get?'+urllib.urlencode({'key':k})).read())
	if 'success' not in ret or ret['success']!="true" or ret['value']!=test[k].decode('utf8'):
		print 'get fail'
os.system('./stopserver -p')
os.system('./startserver -p')

ret=json.loads(urllib2.urlopen('http://127.0.0.1:5644/kvman/dump').read())
for k,v in ret:
	if k.encode('utf8') not in test or test[k.encode('utf8')]!=v.encode('utf8'):
		print 'dump fail'
if len(ret)!=len(test):
		print 'dump fail'

os.system('./stopserver -p')
os.system('./stopserver -b')
