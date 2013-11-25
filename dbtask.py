# dbtask.py
# Created by frankV
# this file is part of PythonPubCrawler 
# https://github.com/frankV/pythonpubcrawl

""" dbtask.py -- database interface  """

import os
from sqlalchemy import *
from sqlalchemy import schema, types
from sqlalchemy.ext.declarative import declarative_base  

# NEED TO UPDATE MYSQL DB!
db_url = os.environ.get('DATABASE_URL')
engine = create_engine(db_url)
Base = declarative_base(engine)

meta = Base.metadata

file_paths = Table('file_paths', meta,
    Column('table_id', Integer, primary_key = True),
    Column('fullpath', String(255)),
    Column('filename', String(255)),
    Column('extension', String(255)),
    Column('created', String(255)),
    Column('modified', String(255)),
    Column('size', Integer),
    Column('owner', Integer),
    Column('permissions', Integer),
    Column('md5', Binary),
    mysql_engine='InnoDB',
)
file_paths.drop(engine, checkfirst = True)
file_paths.create(engine, checkfirst = True)

def push_to_db(fullpath, fileInfo):
# key is fullpath and filename
# [  0	 ,	 1    ,	    2	 ,    3      ,	 4   ,	 5    ,	      6      ]
# ['name', 'extension', 'created', 'modified', 'size', 'owner', 'permissions']
    
    #print "inserting ", fullpath, fileInfo
    
    i = file_paths.insert()
    
    i.execute(  fullpath    = str(fullpath), 
                filename    = str(fileInfo[0]),
                extension   = str(fileInfo[1]),
                created     = str(fileInfo[2]),
                modified    = str(fileInfo[3]),
                size        = fileInfo[4],
                owner       = fileInfo[5],
                permissions = fileInfo[6],
                md5         = fileInfo[7]
             )
