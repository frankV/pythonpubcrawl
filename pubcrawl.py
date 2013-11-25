# pubcrawl.py
# Created by frankV
# PythonPubCrawler 
# https://github.com/frankV/pythonpubcrawl

""" pubcrawl.py -- main  """

import os, argparse, collections, yaml, re, fnmatch
import cPickle as pickle

from dbtask import *
from filemeta import *

""" argparse options

usage: pubcrawl.py [-h] [-v] [-d] [-f] directory [settings]

py pub crawler, stumbles through a given directory and stores metadata for
every file it finds.

positional arguments:
    directory      directory to use
    settings       settings.yaml file location (optional)

optional arguments:
    -h, --help     displays help menu
    -v, --verbose  verbose output from crawler
    -d, --dump     dumps and replaces existing dictionaries
    -f, --fake     crawl only, nothing stored to DB

"""
parser = argparse.ArgumentParser(
        description='py pub crawler, stumbles through a given directory and \
        stores metadata for every file it finds.', fromfile_prefix_chars="@" )
parser.add_argument('-v', '--verbose', 
      help='verbose output from crawler', 
      action="store_true")
parser.add_argument('-d', '--dump', 
      help='dumps and replaces existing dictionaries', 
      action="store_true")
parser.add_argument('-f', '--fake', 
      help='crawl only, nothing stored to DB',
      action="store_true")
parser.add_argument('directory', help='directory to use', action='store')
parser.add_argument('settings', nargs='?', 
      help='settings.yaml file location (optional)', action='store')

args = parser.parse_args()          # parse arguments

if args.verbose: verbose = True     # verbose output
else: verbose = False

if args.dump: dump = True           # dump; will override any existing
else: dump = False                  # dictionaries and drop existing tables

if args.fake: fake = True           # fake; crawl only, will not update 
else: fake = False

newFiles = 0                        # GLOBAL "newFiles"; new files
delFiles = 0                        # GLOBAL "delFiles"; not found
updFiles = 0                        # GLOBAL "updFiles"; updated

files = {}                          # GLOBAL "files"; main dictionary
# extensions = collections.defaultdict(int)


""" args.settings
optional argument "settings" defines a yaml file that can be used to specify 
certain rules for the crawler to follow in specific directories including 
creation of database tables, column specifications, etc.

RULES = project_name, project_directory(s), categories, nomenclature
"""

if args.settings and args.settings[-5:len(args.settings)] == '.yaml':
  import yamlRx
  yamlRx.verify(args.settings)
  settings_stream = open(args.settings, 'r')
  settingsMap = yaml.safe_load(settings_stream)
  yaml_stream = True
  if verbose: print 'using yaml file: ' + args.settings
  print  yaml.load(settings_stream)
else:
  print 'YAML Parse Error: check settings file'
  prompt = raw_input('If you continue, settings will not be applied.\
                      \nContinue? (q = quit) ')
  if prompt == 'q':
    sys.exit()

# ---------------------------------------------------------------------------- #
#   function - crawlDir
#   using os.path, crawl directory and all sub-directories, for each file found
#   add filename, full path and other meta data to dict "files".
# ---------------------------------------------------------------------------- #
def crawlDir():
  # globals
  global newFiles, files, extensions, verbose, dump, fake

  # directory to crawl = directory passed in by command line
  directory = args.directory

  # dictionary selection
  #  - if dump flag; replace existing dictionaries
  #  - if pickle found use; else start new dictionaries
  if not dump:
      if pickleTry():
          pickleLoad()
          if verbose: print 'Using existing dictionary...\n'
      else:
          if verbose: print 'Starting new dictionary...'
  else:
      if pickleTry():
          if verbose: print 'Replacing existing dictionaries.' 

  """ this is UGLY! fix this soon!!! """
  dirList = []
  if args.settings:
      i = 0
      print 'loaded settings for: '
      for project in settingsMap:
        print project + ', ' + settingsMap[project][0]["proj_directory"]
        dirList.append(settingsMap[project][0]["proj_directory"])
        for cats in settingsMap[project][1]['categories']:
            for cat in cats:
                i+=1
                print str(i) + ':' + cat,
      print '\n'

      print 'rules loaded for:'
      for project in settingsMap:
        print settingsMap[project][0]["proj_directory"]
        mainDir = settingsMap[project][0]["proj_directory"]

        for cats in settingsMap[project][1]['categories']:
            for cat in cats:
                dirList.append(mainDir + '/' + cat)
      print dirList
  """ seriously ^ that does not a pythonist make! """


  prompt = raw_input('\nContinue? (q = quit) ')
  if prompt == 'q':
    sys.exit()

  # when the dictionary 'files' already exists
  # for each file, check if file is not already in dict "files"
  # then store file meta data accordingly
  if verbose: print  'Crawling:', directory
  for dirname, dirnames, filenames in os.walk(directory, topdown=True):

    if verbose: print '\nsearching... ' + dirname

    if dirname in dirList:
      print 'using rules for' + dirname
      prompt = raw_input('press any key to continue')
    
    for filename in filenames:
        if not inFiles(os.path.abspath(os.path.join(dirname, filename))) and \
        not ignoredFiles(filename) and \
        not ignoredDirectories(dirname):

          # prompt = raw_input('I\'m about to drop that table like it\'s hot')

          # fullpath = os.path.dirname(os.path.realpath(filename))
          fileobject = FileMeta(os.path.abspath(os.path.join(dirname, \
                                filename)), filename)

          newFiles += 1   # number of new files added to dict
          # extensions[os.path.splitext(filename)[1].lower()] += 1
          files[fileobject.fullPathFileName] = fileobject.fileMetaList()

          if not fake: 
            dbStore(fileobject.fullPathFileName, fileobject.fileMetaList())
          if verbose: 
            print '+   added...', fileobject.fullPathFileName

        # file already listed in files dict
        elif inFiles(os.path.abspath(os.path.join(dirname, filename))):
            # update file meta data and verify file still exists
            if verbose: print '\n--- file already found ---',
            updateFiles(os.path.abspath(os.path.join(dirname, filename)))


# ---------------------------------------------------------------------------- #
#   function - ignoredFiles
#   checks if file is in ignore list
#
#   function - ignoredDirectories
#   checks if directory is in ignore list
# ---------------------------------------------------------------------------- #
def ignoredFiles(filename = None):
  ignore = [ '.DS_Store', '*.pyc', '__init__.py', '*.p' ]
  
  for ignored_file in ignore:
    if re.search(fnmatch.translate(ignored_file), filename):
      return True

  return False

def ignoredDirectories(directory = None):
  ignore = [ '.git/*', 'env/*' ]

  for ignored_directory in ignore:
    if re.search(fnmatch.translate(ignored_directory), directory):
      return True

  return False

# ---------------------------------------------------------------------------- #
#   function - inFiles
#   checks dict for existence of filename with path
# ---------------------------------------------------------------------------- #
def inFiles(fullPathFileName = None):
    # check file is not default case
    if fullPathFileName != None:
         if fullPathFileName in files: return True
         else: return False

# ---------------------------------------------------------------------------- #
#   function - verifyFiles
#   checks dict for existence of filename with path
# ---------------------------------------------------------------------------- #
def verifyFiles():
    global delFiles, vebose
    for existingFile in files.keys():
        if not os.path.exists(existingFile):
            if verbose: print '- removed:', exfile
            del files[existingFile]
            delFiles += 1

# ---------------------------------------------------------------------------- #
#   function - dbStore
#   stores file data to database -- uses imported push_to_db
# ---------------------------------------------------------------------------- #
def dbStore(fullpath, fileInfo):
    #global fake
    #if not fake:
        push_to_db(fullpath, fileInfo)

# ---------------------------------------------------------------------------- #
#   function - updateFiles
#   verify prev collected file meta data and update accordingly
# ---------------------------------------------------------------------------- #

# [  0	 ,	    1	    ,	    2	   ,     3	   ,	 4	 ,	  5	  ,		    6 	   ]
# ['name', 'extension', 'created', 'modified', 'size', 'owner', 'permissions']

"""
there has to be a MORE pythonic way to do this! there is no need to check each
item in the file list explicitly. use len(fileinfo) and do this more 
efficiently! that way you can continue to add members to the filemeta class 
and not have to continue altering this damn function each time
"""

def updateFiles(fullPathFileName = None):
	global updFiles
	updated = False
	
    # check filename is not default case
	if fullPathFileName != None and fullPathFileName in files:
	    exfileInfo = []
        exfileInfo = files.get(fullPathFileName, "empty")
        
        try: # file stat
            st = os.stat(fullPathFileName)
        except IOError, TypeError:
            print "failed to get file info"
            return
        else:
            # get file size and created date
            created = time.ctime(os.path.getctime(fullPathFileName))
            modified = time.ctime(os.path.getmtime(fullPathFileName))
            size = st[ST_SIZE]
            owner = st[ST_UID]
            permissions = oct(st[ST_MODE])[-3:]

        fileInfo = ['filename', 'ext', created, modified, size, owner, permissions]
    
        if fileInfo[2] != exfileInfo[2]:
            exfileInfo[2] = fileInfo[2]
            updated = True
        if fileInfo[3] != exfileInfo[3]:
            exfileInfo[3] = fileInfo[3]
            updated = True
        if fileInfo[4] != exfileInfo[4]:
            exfileInfo[4] = fileInfo[4]
            updated = True
        if fileInfo[5] != exfileInfo[5]:
            exfileInfo[5] = fileInfo[5]
            updated = True
        if fileInfo[6] != exfileInfo[6]:
            exfileInfo[6] = fileInfo[6]
            updated = True
        
        if updated is True:
            if verbose: print '- updated:', fullPathFileName
            updFiles += 1
            




# ---------------------------------------------------------------------------- #
#   function - pickleDump
#   saves "files" and "extensions" dict to a file
# ---------------------------------------------------------------------------- #
def pickleDump():
  global files, extensions, verbose, fake
  if not fake:
      if verbose: print 'pickling...'
      pickle.dump( files, open( "filesdict.p", "wb" ) )
      # pickle.dump( extensions, open( "extensionsdict.p", "wb" ) )


# ---------------------------------------------------------------------------- #
#   function - pickleLoad
#   loads "files" and "extensions" dict from a file
# ---------------------------------------------------------------------------- #
def pickleLoad():
  global files, extensions, verbose
  cwd = os.getcwd()

  fileDictPickle = str(cwd) + '/filesdict.p'
  # extDictPickle = str(cwd) + '/extensionsdict.p'

  if verbose: print 'Loading files...'
  files = pickle.load( open( fileDictPickle, "rb" ) )
  # if verbose: print 'Loading extensions...'
  # extensions = pickle.load( open( extDictPickle, "rb" ) )


# ---------------------------------------------------------------------------- #
#   function - pickleTry
#   checks if pickled files already exist
# ---------------------------------------------------------------------------- #
def pickleTry():
  global verbose
  cwd = os.getcwd()

  try:
    with open(cwd+'/filesdict.p') as f:
        pass
        if verbose: print 'pickle found'
        return True
  except IOError as e:
    print 'pickle not found'
    return False

# ---------------------------------------------------------------------------- #
#   function - printExtensions
#   prints list of extensions along with number of files of that type
# ---------------------------------------------------------------------------- #
def printExtensions():
  global verbose
  for key,value in extensions.items():
      if verbose: print 'Extension: ', key, ' ', value, 'items'


# ---------------------------------------------------------------------------- #
#   function - print_files
#   prints list of filenames from dict "files"
# ---------------------------------------------------------------------------- #
def print_fileNames():
  newLine = '\n'
  count = 0
  for key,value in files.iteritems():
      count += 1
      print count,'-', key, value, '\n'
  return (newLine)


# ---------------------------------------------------------------------------- #
#   function - print_dictTotal(D) ; where D is a python dict object
#   prints all elements in dictionary
# ---------------------------------------------------------------------------- #
def print_dictTotal(D):
    return (len(D))



# ---------------------------------------------------------------------------- #
#   main program
# ---------------------------------------------------------------------------- #


crawlDir()
if not fake: verifyFiles()

if verbose:
    print '\n'
    print 'Added:  ', newFiles, 'new file(s) to list.\n'
    print 'Removed:', delFiles, 'file(s) from list.\n'
    print 'Updated:', updFiles, 'of', print_dictTotal(files), 'file(s) in list.\n'
    print 'Total:  ', print_dictTotal(files), 'entries in list.\n'

pickleDump()

