import rdflib
from rdflib import *  
from rdflib import plugin
from sha1_store import *                   
from stringstore import *
from index_manager import *

'''simple importer''' 
class TygerstoreImporter2(object):
    
    def setup(path="./data_lubm/"):
        plugin.register( 'Sha1Store', rdflib.store.Store,'sha1_store', 'Sha1Store')
        self.g = Graph(store="Sha1Store")
        self.g.stringstore = Stringstore(mode="sha1", store="tc", path)
        self.g.index_manager = IndexManager()
        
    def import_file(file, format="nt"):
        self.g.parse(file, format=format)
        
class TygerstoreImporter(object):
    
    def __init2__(self, file, format="nt"):
        self.file = file               
        self.format = format
        plugin.register( 'Sha1Store', rdflib.store.Store,'sha1_store', 'Sha1Store')
        self.g = Graph(store="Sha1Store")   
        self.outfile = open('data_lubm/spo.hxid', 'w')        
        self.stringstore = Stringstore(mode="sha1", store="tc", path="./data_lubm/")
        self.g.store.outfile = self.outfile
        self.g.store.importer = self
        #self.index_manager = IndexManager()                   

   
    def parse(self):
        return self.g.parse(self.file, format=self.format)
        
 
        
        
    def add_to_store(self):
        print "add to store called"
        for (s,p,o) in self.g:                 
            subj = str(s)
            pred = str(p)
            obje = str(o)
            triple = (self.stringstore.add(subj), self.stringstore.add(pred), self.stringstore.add(obje))
            self.outfile.write("%s %s %s" % (sub,pred,obj))                               
            #print "adding triple: %s %s %s" % triple
            #self.index_manager.add_to_all_indexes(triple) 
            
            
    def close():
        self.stringstore.close()
        self.index_manager.close()
        self.outfile.close()

import time       
class TygrstoreHXIDImporter(object):
    
    def init_index_manager(self):
        self.index_manager = IndexManager()
        
    def import_all_hxid(self):
                
        for idx in self.index_manager.unique_indexes:
            print "importing from data_lubm/%s.sorted.hxid" % idx.name
            self.import_hxid("data_lubm/%s.sorted.hxid" % idx.name, idx)
            
    def import_hxid(self, filename, index):
        self.start_time = time.time()
        f = file(filename, "r")
        linecount = 0
        try:
            for line in f: 
                linecount += 1 
                rawtriple = line.strip()
                try:
                    index.add_triple( (rawtriple[0:20], rawtriple[20:40], rawtriple[40:60]))
                except:
                    "print error importing triple line %s" % binascii.hexlify(rawtriple) 
                if (linecount % 200000) == 0:
                    print "line %i from %s" % (linecount, filename)
                    print "time per entry: %f ms" %  ( ( time.time() - self.start_time) / linecount  * 1000 ) 
        except KeyError:
            print "key error for file %s on line %s" % (filename, linecount)