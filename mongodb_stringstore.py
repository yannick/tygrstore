__author__ = 'Yannick Koechlin'
__email__ = 'yannick@koechlin.name'
import kyotocabinet as kc
import os
from stringstore import Stringstore 
import logging
from helpers import *                             
import pymongo
from bson import ObjectId  
import hashlib as hl
from bson.binary import Binary
from pymongo import Connection    
class MongoDBStringstore(Stringstore):
    
    def __init__(self, config_instance):
        self.logger = logging.getLogger('MongoDBStringstore')
        self.logger.debug("init MongoDBStringstore")
        self.config_instance = config_instance                                                    
        self.collection_name = config_instance.get("mongodb", "stringstore_collection") 
        self.db_name = config_instance.get("mongodb", "stringstore_db")
        self.host = config_instance.get("mongodb", "host")
        self.port = config_instance.getint("mongodb", "port")  
        
        self.connection = Connection(self.host, self.port) 
        self.db = self.connection[self.db_name]
        self.collection = self.db[self.collection_name]
        self.id_class = eval(self.config_instance.get("mongodb", "id_class"))
        super(MongoDBStringstore, self).__init__(config_instance)
        # setting new_id to counter or sha1_hexidgest_for according to the mode
        self.logger.debug("KyotoCabinetStringstore initialized")
    
    def __del__(self):
        pass
    
    def counter(self):
        pass
           
    '''converts the id to the string'''   
    def id2s(self, an_id):
        return self.collection.find_one( self.id_class(an_id, 5) )["n3"]
    
    '''converts the string to the id'''    
    def s2id(self,a_string):        
        if a_string is None: return None
        #TODO: REMOVE
        return hl.md5(a_string).digest()
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
    @memoized
    def add_string(self,a_string):
        check = self.collection.find_one({"n3":a_string})
        if check:
            return check["_id"].binary                    
        objectid = self.collection.insert({"n3":a_string})
        if objectid:
            return objectid.binary
        else:
            raise KeyError("String could not be added")
    
    def next_id(self):
        pass
         
    def close(self):
        self.db.close()
           
    def __len__():
        return len(self.db)
        
       