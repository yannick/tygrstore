import os                         
import pymongo as pm
import pymongo 
from bson.binary import Binary
      
from pymongo import Connection
import binascii
import logging
from index import *
from helpers import * 
from itertools import *  

'''represents a MongoDB backed spo-type index'''
class KVIndexMongo(KVIndex):
   
    def __init__(self,config_file, name="spo"): 
        
        self.is_open = INDEX_CLOSED
        self.config_file = config_file          
        self.only_distinct = True
        #setup_reordering_decorators and set keylength
        super(KVIndexMongo, self).__init__(self.config_file,name, reorder=False) 
        
        self.connection = Connection(config_file.get("mongodb", "host"), config_file.getint("mongodb", "port")) 
        #we us always input reordering!
        self.db = self.connection[self.input_ordering]
                       
        self.levels = []       
        self.filename_prefix = name
        self.is_open = self.open()
        self.shared = False
     
# tygrstore api methods
    def open(self):
        if self.is_open == INDEX_OPEN:
            return INDEX_OPEN
        for lvl in range(0,len(self.internal_ordering)):
             collection = self.db["level"+str(lvl)]                           
      
             self.logger.debug("opening: %s" % (str(collection)))  
             
             self.levels.append(collection)          
        self.is_open = INDEX_OPEN
        return self.is_open  



    def close(self):
        del self.connection
        return INDEX_CLOSED

    def delete(self):
        '''deletes all databases'''
        raise NotImplemented
            
    
          
   
                    
    def add_triple(self, triple):
        #sid,pid,oid = triple   
        
        mongo_hash = self.to_mongo_hash(triple)
        
        #check if its already in there   
        if self.levels[-1].find_one( mongo_hash ):            
            self.logger.debug("triple/quad already in the store: %s" % str(triple)) 
        else:
            try:                                                          
                #self.logger.debug("trying to set %s" % binascii.hexlify(full_key))
                self.levels[-1].insert(mongo_hash)
                for lvl in range(0,len(self.levels)-1):
                    #give only the relevant combination, indexes are maintained by mongodb!  
                    collection = self.levels[lvl] 
                    for keypart in combinations("spo",lvl + 1):
                        #check if the key exists in the database, if yes, load it and add 1 to selectivity 
                        insert_hash = dict((k,Binary(triple[self.input_ordering.index(k)])) for k in keypart)
                        existing = collection.find_one(insert_hash)
                        if existing:
                            #increment the selectivity by 1
                            collection.update(existing, {'$inc' : {"selectivity":1}}) 
                        else:
                            insert_hash["selectivity"] = 1
                            collection.insert(insert_hash)
            except Exception, e:
                self.logger.error("inserting triple/quad failed! %s" % str(triple)) 
                print e 
                                
            
            
    def add_triple_fast(self, triple):
        #sid,pid,oid = triple   
        mongo_hash = self.to_mongo_hash(triple)
        try:                                                          
            #self.logger.debug("trying to set %s" % binascii.hexlify(full_key))
            self.levels[-1].insert(mongo_hash)         
        except Exception, e:
            self.logger.error("inserting triple/quad failed! %s" % str(triple)) 
            print e          
        
    def __len__(self):
        return self.levels[-1].count() 
     
       
        
    '''get the selectivity count'''    
    def selectivity_for_triple(self, triple): 
        mongo_hash = self.to_mongo_hash(triple)
        lvl = len(mongo_hash)-1
        cur = self.levels[-1].find(mongo_hash)
        return cur.count()          
                            
                          
    def ids_for_triple(self,triple, num_records=-1, searched_natural=None):
        #sid,pid,oid = triple
        mongo_hash = self.to_mongo_hash(triple) 
        if not searched_natural:
            searched_natural = self.internal_ordering[triple.index(None)]
        return self.generator_for_searchstring_with_jump(mongo_hash,searched_natural, num_records=num_records)
        
     
    def maintenance(self):
        #ensure the indices
        lvl = self.levels[-1]
        lvl.create_index([("s", pymongo.ASCENDING), ("p", pymongo.ASCENDING)], name="spoindex")
        lvl.create_index([("s", pymongo.ASCENDING), ("o", pymongo.ASCENDING)], name="sopindex")
        lvl.create_index([("o", pymongo.ASCENDING), ("p", pymongo.ASCENDING)], name="opsindex")
        lvl.create_index([("o", pymongo.ASCENDING), ("s", pymongo.ASCENDING)], name="ospindex")
        lvl.create_index([("p", pymongo.ASCENDING), ("o", pymongo.ASCENDING)], name="posindex")
        lvl.create_index([("p", pymongo.ASCENDING), ("s", pymongo.ASCENDING)], name="psoindex")   

        
        
        pass
        

#helpers 
    def to_mongo_hash(self, triple):
        mongo_hash = {}
        for i in range(len(self.input_ordering)):
            if triple[i]:
                mongo_hash[self.input_ordering[i]] = Binary(triple[i])
        return mongo_hash
                   
#generators    
        

    def generator_for_searchstring_with_jump(self, mongo_hash,searched_natural, num_records=-1):
        #print searchstring
        collection = self.levels[-1]  
        # , { searched_natural : 1}
        #mongo_hash[searched_natural] = { '$exists' : True }
        cursor = collection.find(mongo_hash) 
        cursor.sort(searched_natural)
        nextid = None 
        result = None
        for mongo_doc in cursor:
            if self.only_distinct and result == str(mongo_doc[searched_natural]):
                continue
            result = str(mongo_doc[searched_natural])
            #primitive jump emulation!       
            if nextid:
                if nextid < result:
                    continue
            #import pdb; pdb.set_trace() 
            nextid = yield( result )
        
