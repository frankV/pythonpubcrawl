# pickler.py
# Created by frankV
# this file is part of PythonPubCrawler 
# https://github.com/frankV/pythonpubcrawl

""" pickler.py -- archive serializer  """

import os, collections
import cPickle as pickle


# ---------------------------------------------------------------------------- #
#   function - pickleDump
#   saves "files" and "extensions" dict to a file
# ---------------------------------------------------------------------------- #
def pickleDump():
  global files, extensions

  print 'pickling...'
  pickle.dump( files, open( "filesdict.p", "wb" ) )
  pickle.dump( extensions, open( "extensionsdict.p", "wb" ) )


# ---------------------------------------------------------------------------- #
#   function - pickleLoad
#   loads "files" and "extensions" dict from a file
# ---------------------------------------------------------------------------- #
def pickleLoad():
  files = {}
extensions = collections.defaultdict(int)
  cwd = os.getcwd()

  fileDictPickle = str(cwd) + '/filesdict.p'
  extDictPickle = str(cwd) + '/extensionsdict.p'

  print 'Loading files...'
  files = pickle.load( open( fileDictPickle, "rb" ) )
  print 'Loading extensions...'
  extensions = pickle.load( open( extDictPickle, "rb" ) )


# ---------------------------------------------------------------------------- #
#   function - pickleTry
#   checks to see if pickled files already exist
# ---------------------------------------------------------------------------- #
def pickleTry():
  cwd = os.getcwd()

  try:
    with open(cwd+'/filesdict.p') as f:
        pass
        print 'pickle found'
        return True
  except IOError as e:
    print 'pickle not found'
    return False
