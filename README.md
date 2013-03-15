Python Pub Crawl
==========

An enhanced *Python - Directory Archiver*. "Stumbles" through a given path and it's sub-directories, creates a persistent dictionary archive and simultaneously keeps a database up to date with changes. Perfect for file servers with a web-interface. Can link to download scripts (examples coming soon).


###Usage
------------
Help and Command Summary
<pre>
$ python pubcrawl.py -h
usage: pubcrawl.py [-h] [-v] [-d] [-f] directory

py pub crawler, stumbles through a given directory and stores metadata for every file it finds.

positional arguments:
  directory             directory to start crawl
  config/settings.yaml  settings file location (optional)

optional arguments:
  -h, --help     show this help message and exit
  -v, --verbose  verbose output from crawler
  -d, --dump     dumps and replaces existing dictionaries
  -f, --fake     crawl only, nothing stored to DB
</pre>

Crawl - verbose and dump(create new archives)
<pre>
$ python pubcrawl.py /directory/where/crawl/will/start/ -v -d
pickle found
Replacing existing dictionaries.
Continue? (q = quit)

Searching... /directory/where/crawl/will/start/
+ new add: /directory/where/crawl/will/start/file1.ex
+ new add: /directory/where/crawl/will/start/file2.ex
+ new add: /directory/where/crawl/will/start/file3.ex

Added:   3 new files to list.
Removed: 0 files from list.
Updated: 0 of 3 files in list.
Total:   3 entries in list.
</pre>

Crawl - verbose (existing archives)
<pre>
$ python pubcrawl.py /directory/where/crawl/will/start/ -v
pickle found
Loading files...
Loading extensions...
Using existing dictionary...

Continue? (q = quit)

Searching... /directory/where/crawl/will/start/
--- file already found ---
--- file already found ---
--- file already found ---

Added:   0 new files to list.
Removed: 0 files from list.
Updated: 0 of 3 files in list.
Total:   3 entries in list.
</pre>

###Dependencies
------------
+ [SQLAlchemy](http://www.sqlalchemy.org, 'sqlachemy')  - Object relational mapping. Interface for database.
+ [MySQL-python](http://sourceforge.net/projects/mysql-python/, 'mysql-python') - Best to install from pip
+ [PyYAML](http://pyyaml.org, 'pyyaml') - a YAML parser and emitter for the Python programming language.
