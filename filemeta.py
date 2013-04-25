# filemeta.py
# Created by frankV
# PythonPubCrawler 
# https://github.com/frankV/pythonpubcrawl

""" filemeta.py -- base class for file meta data  """



class FileMeta(object):
	"""docstring for FileMeta"""
	def __init__(self, arg):
		super(FileMeta, self).__init__()

		self.fullPathFileName = os.path.join(dirname, filename)
		self.filename = filename
		self.ext = os.path.splitext(filename)[1].lower()
		self.created = time.ctime(os.path.getctime(fullPathFileName))
	    self.modified = time.ctime(os.path.getmtime(fullPathFileName))
	    self.size = st[ST_SIZE]
	    self.owner = st[ST_UID]
	    self.permissions = oct(st[ST_MODE])[-3:]