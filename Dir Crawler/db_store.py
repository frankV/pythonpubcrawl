from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base  

db_url = 'mysql+mysqldb://root:root@localhost/file_metadata'
engine = create_engine(db_url)
Base = declarative_base(engine)

metadata = Base.metadata

file_table = Table('file_paths', metadata)

def push_to_db(fullpath, fileInfo):
# key is fullpath and filename
# [  0	 ,	    1	  ,	    2	 ,	   3	 ,	 4	 ,	 5	  ,		  6 	 ]
# ['name', 'extension', 'created', 'modified', 'size', 'owner', 'permissions']
    
    print "inserting ", fullpath, fileInfo
    
    i = file_table.insert()
    
    i.execute(  fullpath    = str(fullpath), 
                filename    = str(fileInfo[0]),
                extension   = str(fileInfo[1]),
                created     = str(fileInfo[2]),
                modified    = str(fileInfo[3]),
                size        = str(fileInfo[4]),
                owner       = str(fileInfo[5]),
                permissions = str(fileInfo[0]),
             )
