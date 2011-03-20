__author__ = 'Yannick Koechlin'
__email__ = 'yannick@koechlin.name'
#     This file is part of Tygrstore.
# 
#     Tygrstore is free software: you can redistribute it and/or modify
#     it under the terms of the GNU Affero General Public License as published by
#     the Free Software Foundation, either version 3 of the License, or
#     (at your option) any later version.
# 
#     Tygrstore is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU Affero General Public License for more details.
# 
#     You should have received a copy of the GNU Affero General Public License
#     along with Tygrstore.  If not, see <http://www.gnu.org/licenses/>.


import os                         
import pymongo as pm
import pymongo 
from bson.binary import Binary
from bson import ObjectId      
from pymongo import Connection
import binascii
import logging
from index import *
import functools
from helpers import * 
from itertools import *  
                                       
#monkeypatch...
Binary.binary = property(Binary.__str__)  

'''represents a MongoDB backed spo-type index'''
class KVIndexMongo(KVIndex):
   
    def __init__(self,config_file, name="spo"): 
        
        self.is_open = INDEX_CLOSED
        self.config_file = config_file          
        self.only_distinct = True
        #setup_reordering_decorators and set keylength
        super(KVIndexMongo, self).__init__(self.config_file,name, reorder=False)  
        #id_class: ObjectId or Binary or int

        self.levels = []       
        self.filename_prefix = name
        self.is_open = self.open()
        self.shared = False 

     
# tygrstore api methods
    def open(self):
        if self.is_open == INDEX_OPEN:
            return INDEX_OPEN             
        self.id_class = eval(self.config_file.get("mongodb", "id_class"))
        self.connection = Connection(self.config_file.get("mongodb", "host"), self.config_file.getint("mongodb", "port")) 
        #we us always input reordering!
        self.db = self.connection[self.config_file.get("mongodb", "db")]   
        self.collection = self.db[self.config_file.get("mongodb", "collection")]                                        
        self.logger.debug("opening: %s with %s ordering" % (str(self.collection), self.input_ordering ))                       
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
        try:                                                          
            #self.logger.debug("trying to set %s" % binascii.hexlify(full_key))
            self.collection.update(mongo_hash)         
        except Exception, e:
            self.logger.error("inserting triple/quad failed! %s" % str(triple)) 
            print e          
        
    def __len__(self):
        return self.collection.count() 
     
       
        
    '''get the selectivity count'''    
    def selectivity_for_triple(self, triple): 
        mongo_hash = self.to_mongo_hash(triple)
        cur = self.collection.find(mongo_hash)
        return cur.count()          
                            
                          
    def ids_for_triple(self,triple, num_records=-1, searched_natural=None):
        #sid,pid,oid = triple
        mongo_hash = self.to_mongo_hash(triple) 
        if not searched_natural:
            raise NotImplemented("you need to provide the searched_natural for now!")
            #searched_natural = self.internal_ordering[triple.index(None)]
        return self.generator_for_searchstring_with_jump(mongo_hash,searched_natural, num_records=num_records)
        
     
    def maintenance(self):
        #ensure the indices
        lvl = self.collection
        lvl.create_index([("s", pymongo.ASCENDING), ("p", pymongo.ASCENDING)], name="spoindex")
        lvl.create_index([("s", pymongo.ASCENDING), ("o", pymongo.ASCENDING)], name="sopindex")
        lvl.create_index([("o", pymongo.ASCENDING), ("p", pymongo.ASCENDING)], name="opsindex")
        lvl.create_index([("o", pymongo.ASCENDING), ("s", pymongo.ASCENDING)], name="ospindex")
        lvl.create_index([("p", pymongo.ASCENDING), ("o", pymongo.ASCENDING)], name="posindex")
        lvl.create_index([("p", pymongo.ASCENDING), ("s", pymongo.ASCENDING)], name="psoindex")   
        lvl.create_index([("s", pymongo.ASCENDING) ], name="pindex")   
        lvl.create_index([("o", pymongo.ASCENDING) ], name="oindex")   
        lvl.create_index([("p", pymongo.ASCENDING) ], name="pindex")   
        lvl.create_index([("n3", pymongo.ASCENDING) ], name="stringstoreindex")                                                                                              
      
        

#helpers 
    def to_mongo_hash(self, triple):
        mongo_hash = {}
        for i in range(len(self.input_ordering)):
            if triple[i]:
                mongo_hash[self.input_ordering[i]] = self.id_class(triple[i])
        return mongo_hash
               
#generators    
        

    def generator_for_searchstring_with_jump(self, mongo_hash,searched_natural, num_records=-1):
        #print "generator searches variable " + searched_natural 
        #print "mongohash: " + str(mongo_hash)
        cursor = self.collection.find(mongo_hash, {searched_natural:1}) 
        #cursor.hint( [ (self.internal_ordering[0],1), (self.internal_ordering[1],1), (self.internal_ordering[2],1)] )
        #cursor.sort(searched_natural)
        
        nextid = None 
        result = None
        while 1:
            mongo_doc = cursor.next()           
            #if self.only_distinct and result == mongo_doc[searched_natural]:
            #    print "got duplicate"
            #    continue
            result = mongo_doc[searched_natural]
            #primitive jump emulation!         
            if nextid and nextid > result.binary:
                #print str(self) +  self.internal_ordering + "jumping until " + binascii.hexlify(nextid) + " because i was at " + binascii.hexlify(result.binary) 
                mongo_hash.update({searched_natural:{"$gte":Binary(nextid)}})
                cursor = self.collection.find(mongo_hash, {searched_natural:1})
                #cursor.hint( [ (self.internal_ordering[0],1), (self.internal_ordering[1],1), (self.internal_ordering[2],1)] )
                #cursor.sort(searched_natural)                                     
            if type(result) == long: nextid = yield(result) 
            #print "%s %s yields: %s" % (str(self), self.internal_ordering, binascii.hexlify(result.binary))
            nextid = yield( result.binary )
            #print "got nextid: " + binascii.hexlify(nextid) 
