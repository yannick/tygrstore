from index import *
from itertools import *

class IndexManager(object):
        
    def __init(self, naturals="spoc", index_class=KVIndexTC):
        self.naturals = naturals
        self.index_class
        self.indexes = []
        
    
    '''here we instanciate one index class for every permutation of the naturals string
    we also add a (tuple as) key for every sub-tuple (eg. s,  and s,p and s,p,o  for s,p,o,c) with the index
    as value
    ''' 
    def build_indexes(self):
        for p in permutations(self.naturals)           
            index_name = "".join(p)
            an_index = self.index_class(name=index_name)
            for i in range(1,len(p)+1):
                self.indexes[i] = an_index 
                
    #from itertools in python 3.1.2
    def compress(self, data, selectors):
        # ("x", None, "y", None)  -> ("s", "o")
        return (d for d, s in zip(data, selectors) if s           
                                                         
    def level_for_tuple(self, triple):
        return len(self.compress(self.naturals, triple))
    
    '''return the coresponding index for a tuple of a triple/quad'''
    def index_for_tuple(self, triple):
        return self.indexes[self.compress(self.naturals, triple)]    
        
        
#index         
    def string2id(self):
        pass
        
    def id2string(self):
        pass
        
        