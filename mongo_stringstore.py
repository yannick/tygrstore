import kyotocabinet as kc
import os
from stringstore import Stringstore 
import logging
                              
import pymongo
from bson import ObjectId
from pymongo import Connection    
class MongoDBStringstore(Stringstore):
    
    def __init__(self, config_instance):
        self.logger = logging.getLogger('MongoDBStringstore')
        self.logger.debug("init MongoDBStringstore")
        self.config_instance = config_instance                                                    
        self.collection_name = config_instance.get("mongodb", "stringstore") 
        self.db_name = config_instance.get("mongodb", "db")
        self.host = config_file.get("mongodb", "host")
        self.port = config_file.getint("mongodb", "port")  
        
        self.connection = Connection(self.host, self.port) 
        self.db = self.connection[self.db_name]
        self.collection = self.db[self.collection_name]
        
        super(KyotoCabinetStringstore, self).__init__(config_instance)
        # setting new_id to counter or sha1_hexidgest_for according to the mode
        self.logger.debug("KyotoCabinetStringstore initialized")
    
    def __del__(self):
        self.db.close()
        
    '''converts the id to the string'''   
    def id2s(self, an_id):
        return self.collection.find_one( ObjectId(an_id) )["n3"]
    
    '''converts the string to the id'''    
    def s2id(self,a_string):
        if a_string is None: return None
        an_id = self.collection.find_one({"n3":a_string})
        #if the id is not in the store we raise an exception b/c the query will be invalid
        if not an_id:
            raise LookupError("Key not in Store!")
        return an_id["_id"].binary
        
    '''returns true if the string is in the store'''
    def contains_string(self,a_string): 
        if self.collection.find_one({"n3":a_string}):
            return True
        return False
        
                                
    '''adds a string to the store and returns its id'''
    def add_string(self,a_string):                    
        objectid = self.collection.insert({"n3":a_string})
        if key:
            return objectid.binary
        else:
            raise KeyError("String could not be added")
    
    def next_id(self):
        pass
         
    def close(self):
        self.db.close()
           
    def __len__():
        return len(self.db)
        
       