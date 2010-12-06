
from rdflib import *                   
from stringstore import *
from index_manager import *

class TygerstoreImporter(object):
    
    def __init__(self, file, format="n3"):
        self.file = file               
        self.format = format
        self.g = Graph()
        self.stringstore = Stringstore()
        self.index_manager = IndexManager()                   
        
    def parse(self):
        self.g.parse(self.file, format=self.format)
        
        
    def add_to_store(self):
        for (s,p,o) in self.g:
            triple = (self.stringstore.add(str(s)), self.stringstore.add(str(p)), self.stringstore.add(str(o)))                               
            print "adding triple: %s %s %s" % triple
            self.index_manager.add_to_all_indexes(triple) 
            
            
    def close():
        self.stringstore.close()
        self.index_manager.close()
        
        