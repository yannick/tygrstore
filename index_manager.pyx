# cython: profile=True
from index import *
from indexkc import *
from indextc import *
from itertools import *
import logging
from helpers import * 
import functools

      
class IndexManager(object):
        
    def __init__(self, config):
        self.logger = logging.getLogger('IndexManager')
        self.logger.debug("init IndexManager")
        self.config = config
        self.index_class = eval(self.config.get("index", "type"))
        self.logger.debug("using index types of class" + str(self.index_class))
        path = os.path.abspath(self.config.get("database", "path"))        
        self.naturals = self.config.get("database", "naturals")
        #indexes contains duplicates!
        self.indexes = dict()
        self.logger.debug("building indexes") 
        #a list of all indexes  
        self.unique_indexes = []
        self.build_indexes(path)
        
    
    '''here we instanciate one index class for every permutation of the naturals string
    we also add a (tuple as) key for every sub-tuple (eg. s,  and s,p and s,p,o  for s,p,o,c) with the index
    as value. they go into unique_indexes and indexes
    ''' 
    def build_indexes(self, path):        
        for p in permutations(self.naturals):           
            index_name = "".join(p)                          
            #instanciate a new index. equal to ie: an_index = KVIndexTC(config_instance, name="spo")
            an_index = self.index_class(self.config, name=index_name)             
            self.unique_indexes.append(an_index)
            for i in range(1,len(p)+1):
                self.indexes[p[0:i]] = an_index                
        #for (None), (None,None,...) set any index 
        for none_tuple_length in range(1,len(p)+1):
            none_tuple = (None,) * none_tuple_length
            #if we have no given vars it does not matter which index we take     
            self.indexes[none_tuple] = an_index        
                
    #from itertools in python 3.1.2
    def compress(self, data, selectors):
        # ("x", None, "y", None)  -> ("s", "o")
        #return (d for d, s in zip(data, selectors) if s)
        for d, s in zip(data, selectors):
            if s:
                yield d        
                    
    
    '''("x", None, "y", None)  -> ("s", "o")'''                                                 
    def level_for_tuple(self, triple):
        return len(list(self.compress(self.naturals, triple)))

    '''return the coresponding index for a tuple of a triple/quad'''
    def index_for_tuple(self, triple):
        return self.indexes[tuple(self.compress(self.naturals, triple))]    
     
    #@memoized    
    def index_for_ttriple(self, ttriple, var):
        idx_name = tuple(i for i in self.compress(self.naturals, ttriple.ids_as_tuple()))
        idx_name += (self.naturals[ttriple.variables_tuple.index(var)],)
        return self.indexes[idx_name]    
 
    '''index needs to support .count'''
    def selectivity_for_tuple(self, triple):
        #self.logger.debug("get selectivity for tuple: " + str(pp_tuple(triple)))
        #
        # self.logger.debug("using index: " + self.index_for_tuple(triple).filename_prefix)
        return self.index_for_tuple(triple).selectivity_for_triple(triple)
    
    def add_to_all_indexes(self, triple):
        for idx in self.unique_indexes:
            idx.add_triple(triple)  
    
    #here we need to make sure, that var is actually the var which is being resolved
    def ids_for_ttriple(self, ttriple, var): 
        #import pdb; pdb.set_trace()        
        return self.index_for_ttriple(ttriple,var).ids_for_triple(ttriple.ids_as_tuple())
                
    def close(self):
        for idx in self.unique_indexes:
            idx.close()
    
    def __len__(self):
        return len(self.unique_indexes[0])                                           
#index         
    def string2id(self):
        pass
        
    def id2string(self):
        pass
                 
    def __del__(self):
        self.logger.debug("closing all indexes")
        self.close()
