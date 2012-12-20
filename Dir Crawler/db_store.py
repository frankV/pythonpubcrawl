from sqlalchemy import *

db = create_engine('mysql+mysqldb://root:root@localhost/file_metadata')

db.echo = False  # Try changing this to True and see what happens

metadata = BoundMetaData(db)

def push_to_db(fullpath, fileInfo):
# key is fullpath and filename
# [  0	 ,	    1	  ,	    2	 ,	   3	 ,	 4	 ,	 5	  ,		  6 	 ]
# ['name', 'extension', 'created', 'modified', 'size', 'owner', 'permissions']
    
    file_table = Table('file_paths', metadata, autoload=True)
    
    i = file_table.insert()
    
    i.execute(  fullpath    = fullpath, 
                filename    = fileInfo[0],
                extension   = fileInfo[1],
                created     = fileInfo[2],
                modified    = fileInfo[3],
                size        = fileInfo[4],
                owner       = fileInfo[5],
                permissions = fileInfo[0],
             )
