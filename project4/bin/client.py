#!/usr/bin/python
import sys
import urllib
import urllib2
import json
import os
import time
import random


conf = json.loads(open('conf/settings.conf').read())
nquerys = 44 # Thanks to fhq

nserver = len(conf) - 1

urls = []
for i in range(1, nserver + 1):
	 urls.append('http://%s:%s/' % (conf['n%02d' % i], conf['port']))

def checkalive(url): 
	try:
		urllib2.urlopen(url).read()
	except:
		return False
	return True


while True:
	s = raw_input('You should fill ???? in "curl http://xx.xx.xx.xx:xxxx/????"\n')
	for i in urls:
		if checkalive(i):
			os.system('curl "%s%s"' % (i, s))
			break
