from sqlalchemy import *
from sqlalchemy import schema, types
from sqlalchemy.ext.declarative import declarative_base  

# NEED TO UPDATE MYSQL DB!
db_url = 'mysql+mysqldb://valcarce:devsqlFrank@localhost/pubcrawl'
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
    Column('owner', String(255)),
    Column('permissions', Integer),
    mysql_engine='InnoDB',
)
file_paths.drop(engine, checkfirst = False)
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
                size        = str(fileInfo[4]),
                owner       = str(fileInfo[5]),
                permissions = str(fileInfo[0]),
             )
