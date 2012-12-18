import os, time, sys, collections
import cPickle as pickle
from stat import * # ST_SIZE etc

files = {}
extensions = collections.defaultdict(int)
newFiles = 0

# ---------------------------------------------------------------------------- #
#   function - crawlDir
#   using os.path, crawl directory and all sub-directories, for each file found
#   add filename, full path and other meta data to dict "files".
# ---------------------------------------------------------------------------- #
def crawlDir():
  global newFiles
  # directory to crawl
  directory = '/Users/frankvalcarcel/Dropbox'
  
# move this to it's own function #
  if pickleTry():
      pickleLoad()
  
  prompt = raw_input('Press any key. ')
  if prompt == 'q':
    sys.exit()

  print  'Crawling:',
  for dirname, dirnames, filenames in os.walk(directory):
      for subdirname in dirnames:
          print  '.',                         


# add functionality
# update size and date modified
# store file owner and permissions


      # for each file, check if file is not already in dict "files"
      # then store file meta data accordingly
      for filename in filenames:
          if filename not in files['filename']:
            fullPathFileName = os.path.join(dirname, filename)
            files.setdefault('filename',[]).append(str(filename))
            files.setdefault('full_path',[]).append(str(fullPathFileName))
                        
            # new file counter, number of new files added to dict "files"
            newFiles += 1
            
            # assert current filename is not a directory
            if not os.path.isdir(filename):
                try:   
                    # save full path to st
                    st = os.stat(fullPathFileName)
                except IOError:
                    print "failed to get file info"
                else:
                    # get file size and created date
                    files.setdefault('size',[]).append(str(st[ST_SIZE]))
                    files.setdefault('date_created',[]).append (str(time.asctime(time.localtime(st[ST_MTIME]))))


                # strip file extension and add to extensions list
                # increment counter for each extension found
                extensions[os.path.splitext(filename)[1].lower()] += 1
            
            else:
                print filename, ' - file already found'


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


# ---------------------------------------------------------------------------- #
#   function - printExtensions
#   prints list of extensions along with number of files of that type
# ---------------------------------------------------------------------------- #
def printExtensions():
  for key,value in extensions.items():
      print 'Extension: ', key, ' ', value, 'items' 
      
      
# ---------------------------------------------------------------------------- #
#   function - print_files
#   prints list of filenames from dict "files"
# ---------------------------------------------------------------------------- #
def print_files():
  for key,value in files.iteritems():
    print files['filename']





# ---------------------------------------------------------------------------- #
#   main program
# ---------------------------------------------------------------------------- #


crawlDir()
print '\n'
print_files()
print '\n'
printExtensions()
print '\n'
print 'Added: ', newFiles, ' new files to list.\n'
pickleDump()