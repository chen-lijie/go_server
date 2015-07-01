#!/usr/bin/python
import sys
import urllib
import urllib2
import json
import os
import time
import random
import sys


conf = json.loads(open('conf/settings.conf').read())

def RunCommand(cmd):
	port=conf['port']
	keys=conf.keys()
	random.shuffle(keys)
	sessionid=str(random.random())
	if '?' in cmd:
		cmd=cmd+'&session='+sessionid
	else:
		cmd=cmd+'?session='+sessionid
	for servername in keys:
		if servername[0]=='n':
			ip=conf[servername].split(':')[0]
			try:
				response=urllib2.urlopen('http://%s:%s/%s'%(ip,port,cmd)).read().strip()
			except Exception as e:
				continue
			print ip,response
			return response
	else:
		print 'too bad, all servers are dead'

for cmd in sys.argv[1:]:
	RunCommand(cmd)
