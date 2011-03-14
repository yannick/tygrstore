# cython: profile=True
import hashlib  
import binascii
#from tc import *
#import os 
class Stringstore(object):       
        
    def __init__(self, config_file):          
        self.config_file=config_file
        self.mode = self.config_file.get("index", "hashfunction")  
        self.numeric_ids = self.config_file.getboolean("general", "numeric_ids")   
        self.keyfunctions = {"sha1" : self.sha1_hexdigest_for, "counter" : self.counter, "md5" : self.md5_hexdigest_for}            
        # setting new_id to counter or sha1_hexidgest_for according to the mode
        self.get_new_id = self.keyfunctions[self.mode]

        
    '''return a sha1 as hex string'''
    def sha1_hexdigest_for(self,string):
        return hashlib.sha1(string).digest()
    
    def md5_hexdigest_for(self,string):
        return hashlib.md5(string).digest() 
            
        
    def __del__(self):
        self.close()  
    
    def s2id(self, a_str):
        raise NotImplemented
        
    def id2s(self, an_id):
        raise NotImplemented
                                    
    '''returns true if the string is in the store'''
    def contains_string(self,a_str):
        if self.s2id(a_str): return True
        return False
    
    def get_or_add_string(self,a_string):
        if self.contains_string(a_string):
            return self.s2id(a_string)
        else:
            return self.add_string(a_string)
            
    '''in: tuple of s,p,o in ntriple format)     
    out: tuple of keys 

    example for md5 keys:
    in: ('<http://subject>', '<http://object>', '"literal"'
    out :(hl.md5("<http://subject>").digest(), hl.md5("<http://object>").digest(), hl.md5('"literal"').digest()) ) 
    raises an exception if one of the id's is not in the store (but not if any of the )
    '''
    def get_ids_from_tuple(self, triple):
        keys = tuple(self.s2id(i) for i in triple)        
        return keys       

    def get_strings_from_tuple(self, triple):
        return tuple(self.id2s(i) for i in triple)         