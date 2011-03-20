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


# cython: profile=True
from index import *
from indexkc import *
from indextc import *
from indexmongo import *
from itertools import *
import logging
from helpers import * 
import functools

      
class IndexManager(object):
        
    def __init__(self, config):
        self.logger = logging.getLogger('IndexManager')
        self.logger.debug("init IndexManager")
        self.config = config
        self.update_only_one = self.config.getboolean("index_manager", "update_only_one")
        self.index_class = eval(self.config.get("index", "type"))
        self.logger.debug("using index types of class" + str(self.index_class))       
        self.naturals = self.config.get("database", "naturals")
        #indexes contains duplicates!
        self.indexes = dict()
        self.logger.debug("building indexes") 
        #a list of all indexes  
        self.unique_indexes = []
        self.build_indexes()
        
    
    '''here we instanciate one index class for every permutation of the naturals string
    we also add a (tuple as) key for every sub-tuple (eg. s,  and s,p and s,p,o  for s,p,o,c) with the index
    as value. they go into unique_indexes and indexes
    ''' 
    def build_indexes(self):
        #build a list of tuples all permutations of e.g. 'spo' -> ('s','p','o), ('s','o','p')  etc.     
        for p in permutations(self.naturals):
            #the name (in the index known as internal_ordering). e.g. ('s','o','p') -> "sop"           
            index_name = "".join(p)                          
            #instanciate a new index. equal to e.g.: an_index = KVIndexTC(config_instance, name="spo")
            an_index = self.index_class(self.config, name=index_name) 
            #maintain a list of all indexes in unique_indexes            
            self.unique_indexes.append(an_index) 
            #generate the keys for the mapping dict self.indexes and set the index as value
            #e.g. for 'spo'  -> ('s',), ('s','p'), ('s','p','o') as keys
            for i in range(1,len(p)+1):
                self.indexes[p[0:i]] = an_index                
        #for (None), (None,None,...) set any index 
        for none_tuple_length in range(1,len(p)+1):
            none_tuple = (None,) * none_tuple_length
            #if we have no given vars it does not matter which index we take     
            self.indexes[none_tuple] = an_index        
                
    #backport from itertools in python 3.1.2
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

    '''return the coresponding indexes for a tuple of a triple/quad''' 
    @memoized
    def indexes_for_tuple(self, triple):
        #generator for all possible key combinations
        naturals = self.compress(self.naturals, triple) 
        #generator for all permutations
        all_indexes = permutations(x for x in naturals)
        #list of all 
        return [ self.indexes[i] for i in all_indexes  ]  
           
    '''return the coresponding indexes for a tuple of a triple/quad''' 
    @memoized    
    def index_for_ttriple(self, ttriple, var):
        #get all solved naturals in the triple. 
        idx_name = tuple(i for i in self.compress(self.naturals, ttriple.ids_as_tuple())) 
        # add the natural where the variable we are searching for is
        idx_name += (self.naturals[ttriple.variables_tuple.index(var)],) 
        #look the index up in the dict
        return self.indexes[idx_name]    
 
    '''index needs to support .count'''
    def selectivity_for_tuple(self, triple):
        #self.logger.debug("get selectivity for tuple: " + str(pp_tuple(triple)))
        #
        # self.logger.debug("using index: " + self.index_for_tuple(triple).filename_prefix)
        return self.index_for_tuple(triple).selectivity_for_triple(triple)
    
    def add_to_all_indexes(self, triple):
        if self.update_only_one:
            self.unique_indexes[0].add_triple(triple)
        else:
            for idx in self.unique_indexes:
                idx.add_triple(triple)  
    
    #here we need to make sure, that var is actually the var which is being resolved
    def ids_for_ttriple(self, ttriple, var): 
        #import pdb; pdb.set_trace()
        snatural = None 
        if self.update_only_one:  
            snatural = self.naturals[ttriple.variables_tuple.index(var)]    
        return self.index_for_ttriple(ttriple,var).ids_for_triple(ttriple.ids_as_tuple(), searched_natural=snatural)
                
    def close(self):
        if self.update_only_one:
            self.unique_indexes[0].close()
        else: 
            for idx in self.unique_indexes:
                idx.close() 
    
    def __len__(self):
        return len(self.unique_indexes[0])                                           

                 
    def __del__(self):
        self.logger.debug("closing all indexes")
        self.close()
