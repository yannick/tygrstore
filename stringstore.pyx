# cython: profile=True
import hashlib  
import binascii
#from tc import *
#import os 
class Stringstore(object):       
        
    def __init__(self, config_file):          
        self.config_file=config_file
        self.mode = self.config_file.get("index", "hashfunction")
        self.keyfunctions = {"sha1" : self.sha1_hexdigest_for, "counter" : self.counter, "md5" : self.md5_hexdigest_for}            
        # setting new_id to counter or sha1_hexidgest_for according to the mode
        self.get_new_id = self.keyfunctions[self.mode]

        
    '''return a sha1 as hex string'''
    def sha1_hexdigest_for(self,string):
        return hashlib.sha1(string).digest()
    
    def md5_hexdigest_for(self,string):
        return hashlib.md5(string).digest() 
            
    '''return the next int'''    
    def counter(self, string):
        return str(self.s2id.addint("counter", 1))
        
    def __del__(self):
        self.close()