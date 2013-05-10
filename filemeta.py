# filemeta.py
# Created by frankV
# PythonPubCrawler 
# https://github.com/frankV/pythonpubcrawl

""" filemeta.py -- base class for file meta data  """

import os, time, sys, stat

class FileMeta(object):
	"""docstring for FileMeta"""

	def __init__(self, fullPathFileName, filename):
		# super(FileMeta, self).__init__()

		self.fullPathFileName = fullPathFileName
		self.filename = filename
	
	def getExtension(self):
		return os.path.splitext(self.filename)[1].lower()

	def getMetaData(self):
		try: # file stat
			st = os.stat(self.fullPathFileName)
	    except OSError, e:
	        print "failed to get file info"
	    else:
          	# get file size and created date
          	created = time.ctime(os.path.getctime(fullPathFileName))
          	modified = time.ctime(os.path.getmtime(fullPathFileName))
          	size = st[ST_SIZE]
          	owner = st[ST_UID]
          	permissions = oct(st[ST_MODE])[-3:]

	def fileMetaList(self):
		return [self.filename, self.ext, self.created, self.modified, self.size, self.owner, self.permissions]





	 # 	  self.ext = os.path.splitext(filename)[1].lower()
	 # 	  self.created = time.ctime(os.path.getctime(fullPathFileName))
	 #    self.modified = time.ctime(os.path.getmtime(fullPathFileName))
	 #    self.size = st[ST_SIZE]
	 #    self.owner = st[ST_UID]
	 #    self.permissions = oct(st[ST_MODE])[-3:]