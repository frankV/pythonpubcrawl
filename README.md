Python Pub Crawl
==========

An enhanced *Python - Directory Archiver*. "Stumbles" through a given path and it's sub-directories, creates a persistent dictionary archive and simultaneously keeps a database up to date with changes. Perfect for file servers with a web-interface. Can link to download scripts (examples coming soon).


###Usage
------------
<pre>
(env)-bash-3.2$ python dir_crawl.py -h
usage: dir_crawl.py [-h] [-v] [-d] [-f] directory

py pub crawler, stumbles through a given directory and stores metadata for every file it finds.

positional arguments:
  directory      directory to start crawl

optional arguments:
  -h, --help     show this help message and exit
  -v, --verbose  verbose output from crawler
  -d, --dump     dumps and replaces existing dictionaries
  -f, --fake     crawl only, nothing stored to DB
</pre>

###Dependencies
------------
+ [SQLAlchemy](http://www.sqlalchemy.org, 'sqlachemy')  - Object relational mapping. Interface for database.
+ [MySQL-python](http://sourceforge.net/projects/mysql-python/, 'mysql-python') - Best to install from pip
