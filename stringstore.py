import hashlib
from tc import *
import os 
class Stringstore(object):       
    
    
    
    def __init__(self, mode="sha1", store="tc", path="."):
        self.path = path        
        
        
        self.modes = {"sha1" : self.sha1_hexdigest_for, "counter" : self.counter}
        self.stores = {"tc" : self.use_tc }
        
        #init the store 
        #calling the initializer for the kv backend
        self.stores[store]()              
        # setting new_id to counter or sha1_hexidgest_for according to the mode
        self.get_new_id = self.modes[mode] 
   
    '''use tokyo cabinet library'''    
    def use_tc(self):       
        s2id = BDB()
        id2s = BDB()
        #tuning?             

        #print fullpath
        s2id.open("s2id.stringstore.bdb", BDBOWRITER | BDBOREADER | BDBOCREAT) 
        id2s.open("id2s.stringstore.bdb", BDBOWRITER | BDBOREADER | BDBOCREAT) 
        
        self.s2id = s2id
        self.id2s = id2s
        
    '''add a string and get its key'''        
    def add(self, string):
        #check if already in
        try:
            return self.s2id.get(string)
        except KeyError:            
            #we need a new id
            new_id = self.get_new_id(string)
            self.s2id.put(string, new_id )
            self.id2s.put(new_id, string) 
            return new_id
    
    def get(self, key):
        return self.id2s.get(key)
        
    '''return a sha1 as hex string'''
    def sha1_hexdigest_for(self,string):
        return hashlib.sha1(string).hexdigest()
    
    '''return the next int'''    
    def counter(self, string):
        return str(self.s2id.addint("counter", 1))