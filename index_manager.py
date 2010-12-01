from index import *
from itertools import *

class IndexManager(object):
        
    def __init__(self, naturals="spo", index_class=KVIndexTC):
        self.naturals = naturals
        self.index_class = index_class
        self.indexes = dict()
        
    
    '''here we instanciate one index class for every permutation of the naturals string
    we also add a (tuple as) key for every sub-tuple (eg. s,  and s,p and s,p,o  for s,p,o,c) with the index
    as value
    ''' 
    def build_indexes(self):        
        for p in permutations(self.naturals):           
            index_name = "".join(p)
            an_index = self.index_class(name=index_name)
            for i in range(1,len(p)+1):
                self.indexes[p[0:i]] = an_index                
        #for (None), (None,None,...) set any index 
        for none_tuple_length in range(1,len(p)+1):
            none_tuple = tuple(None for i in range(0,none_tuple_length))       
            self.indexes[none_tuple] = an_index        
                
    #from itertools in python 3.1.2
    def compress(self, data, selectors):
        # ("x", None, "y", None)  -> ("s", "o")
        return (d for d, s in zip(data, selectors) if s)            
                                                     
    def level_for_tuple(self, triple):
        return len(list(self.compress(self.naturals, triple)))

    '''return the coresponding index for a tuple of a triple/quad'''
    def index_for_tuple(self, triple):
        return self.indexes[tuple(self.compress(self.naturals, triple))]    
    
                        
#index         
    def string2id(self):
        pass
        
    def id2string(self):
        pass
        
        