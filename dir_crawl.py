import argparse, os, time, sys, collections, getopt
from stat import *
import cPickle as pickle
from db_store import push_to_db


parser = argparse.ArgumentParser(description='py pub crawler, stumbles through a given directory and stores metadata for every file it finds.', fromfile_prefix_chars="@" )
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

args = parser.parse_args()

if args.verbose:
    verbose = True
if args.dump:
    dump = True
if args.fake:
    fake = True

newFiles = 0
delFiles = 0
updFiles = 0

files = {}
extensions = collections.defaultdict(int)

# ---------------------------------------------------------------------------- #
#   function - crawlDir
#   using os.path, crawl directory and all sub-directories, for each file found
#   add filename, full path and other meta data to dict "files".
# ---------------------------------------------------------------------------- #
def crawlDir():
  # globals
  global newFiles, files, extensions, verbose, dump, fake

  # list of file types to ignore
  ignore = [ '.DS_Store' ]

  # directory to crawl
  print 'you want to crawl ' + args.directory + ' ?'
  directory = args.directory
  #directory = '/r/coaps/ftp/samos_pub/data/quick/'

  if not dump:
      if pickleTry():
          pickleLoad()
          print 'Using existing dictionary...\n'
      else:
          print 'Starting new dictionary...'
  
  elif dump:
      print 'replacing existing dictionaries'

  prompt = raw_input('Press any key. ')
  if prompt == 'q':
    sys.exit()

  # when the dictionary 'files' already exists
  # for each file, check if file is not already in dict "files"
  # then store file meta data accordingly
  if files:
      if verbose: print  'Crawling:', directory, '\n'
      for dirname, dirnames, filenames in os.walk(directory, topdown=True):
          
          if verbose: print '\nsearching... ' + dirname
          
          for filename in filenames:
              if filename not in ignore:
                  fullPathFileName = os.path.join(dirname, filename)
                  
                  if not inFiles(fullPathFileName):
                    ext = os.path.splitext(filename)[1].lower()
    
                    try: # file stat
                        st = os.stat(fullPathFileName)
                    except OSError, e:
                        print "failed to get file info"
                    else:
                        # get file size and created date
                        created = time.ctime(os.path.getctime(fullPathFileName))
                        modified = time.ctime(os.path.getmtime(fullPathFileName))
                        size = st[ST_SIZE]
                        owner = st[ST_UID]
                        permissions = oct(st[ST_MODE])[-3:]
    
                        fileInfo = [filename, ext, created, modified, size, owner, permissions]
                        files[fullPathFileName] = fileInfo
                        dbStore(fullPathFileName, fileInfo)
    
                    if verbose: print '+   added...', fullPathFileName
    
                    # new file counter, number of new files added to dict
                    newFiles += 1
                    extensions[os.path.splitext(filename)[1].lower()] += 1
    
                  # file already listed in files dict
                  else:
                    # update file meta data and verify file still exists
                    if verbose: print '\n--- file already found ---',
                    updateFiles(fullPathFileName)



  # if dictionary 'files' does not exist
  # for each file, store meta data accordingly
  else:
      if verbose: print  'Crawling:', directory, '\n'
      for dirname, dirnames, filenames in os.walk(directory, topdown=True):
           if verbose: print '\nsearching... ' + dirname
           for filename in filenames:
             if filename not in ignore:
            
                fullPathFileName = os.path.join(dirname, filename)
                ext = os.path.splitext(filename)[1].lower()
    
                try: # file stat
                    st = os.stat(fullPathFileName)
                except OSError, e:
                    print "failed to get file info"
                else:
                    # get file size and created date
                    created = time.ctime(os.path.getctime(fullPathFileName))
                    modified = time.ctime(os.path.getmtime(fullPathFileName))
                    size = st[ST_SIZE]
                    owner = st[ST_UID]
                    permissions = oct(st[ST_MODE])[-3:]

                    fileInfo = [filename, ext, created, modified, size, owner, permissions]
                    files[fullPathFileName] = fileInfo
                    dbStore(fullPathFileName, fileInfo)
    
                if verbose: print '+ new add:', fullPathFileName
    
                # new file counter, number of new files added to dict "files"
                newFiles += 1
                extensions[os.path.splitext(filename)[1].lower()] += 1



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
    global delFiles
    for exfile in files.keys():
        if not os.path.exists(exfile):
            print '- removed:', exfile
            del files[exfile]
            delFiles += 1

# ---------------------------------------------------------------------------- #
#   function - dbStore
#   stores file data to database -- uses imported push_to_db
# ---------------------------------------------------------------------------- #
def dbStore(fullpath, fileInfo):
    #print "sending ", fullpath, fileInfo
    push_to_db(fullpath, fileInfo)


# ---------------------------------------------------------------------------- #
#   function - updateFiles
#   verify prev collected file meta data and update accordingly
# ---------------------------------------------------------------------------- #

# [  0	 ,	    1	  ,	    2	 ,	   3	 ,	 4	 ,	 5	  ,		  6 	 ]
# ['name', 'extension', 'created', 'modified', 'size', 'owner', 'permissions']

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
  global files, extensions
  print 'pickling...'
  pickle.dump( files, open( "filesdict.p", "wb" ) )
  pickle.dump( extensions, open( "extensionsdict.p", "wb" ) )


# ---------------------------------------------------------------------------- #
#   function - pickleLoad
#   loads "files" and "extensions" dict from a file
# ---------------------------------------------------------------------------- #
def pickleLoad():
  global files, extensions
  cwd = os.getcwd()

  fileDictPickle = str(cwd) + '/filesdict.p'
  extDictPickle = str(cwd) + '/extensionsdict.p'

  print 'Loading files...'
  files = pickle.load( open( fileDictPickle, "rb" ) )
  print 'Loading extensions...'
  extensions = pickle.load( open( extDictPickle, "rb" ) )


# ---------------------------------------------------------------------------- #
#   function - pickleTry
#   checks if pickled files already exist
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

# ---------------------------------------------------------------------------- #
#   function - printExtensions
#   prints list of extensions along with number of files of that type
# ---------------------------------------------------------------------------- #
def printExtensions():
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
verifyFiles()
print '\n'
print 'Added:  ', newFiles, 'new files to list.\n'
print 'Removed:', delFiles, 'files from list.\n'
print 'Updated:', updFiles, 'of', print_dictTotal(files), 'files in list.\n'
print 'Total:  ', print_dictTotal(files), 'entries in list.\n'
#print print_fileNames()
pickleDump()

