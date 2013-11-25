# filemeta.py
# Created by frankV
# PythonPubCrawler 
# https://github.com/frankV/pythonpubcrawl

""" filemeta.py -- base class for file meta data  """

import os, time, sys, hashlib
from stat import *

class FileMeta(object):

	def __init__(self, fullpath, filename):

		self.fullPathFileName = fullpath
		self.filename = filename
		self.extension = self.getExtension()
		self.created = self.getMetaData()[0]
		self.modified = self.getMetaData()[1]
		self.size = self.getMetaData()[2]
		self.owner = self.getMetaData()[3]
		self.permissions = self.getMetaData()[4]
		self.md5 = self.getMD5()
	
	def getExtension(self):
		return os.path.splitext(self.filename)[1].lower()

	def getMetaData(self):
		try:
			st = os.stat(self.fullPathFileName)
		except OSError, e:
			print "failed to get file info"
		else:
			created = time.ctime(os.path.getctime(self.fullPathFileName))
          	modified = time.ctime(os.path.getmtime(self.fullPathFileName))
          	size = st[ST_SIZE]
          	owner = st[ST_UID]
          	permissions = oct(st[ST_MODE])[-3:]
         	return [created, modified, size, owner, permissions]

	def getMD5(self, block_size=128):
		f = open(self.fullPathFileName, 'rb')
		md5 = hashlib.md5()
		while True:
			data = f.read(block_size)
			if not data:
				break
				md5.update(data)
				print md5
				prompt = raw_input('\nmd5?')
				return md5.digest()

	def fileMetaList(self):
		return [self.filename, self.extension, self.created, self.modified,\
		        self.size, self.owner, self.permissions, self.md5]






