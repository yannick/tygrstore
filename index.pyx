__author__ = 'Yannick Koechlin'
__email__ = 'yannick@koechlin.name'
# cython: profile=True
INDEX_OPEN = 42
#todo: 
INDEX_CLOSED = 666  
import binascii
import logging
#LOG_FILENAME = 'index.log'
#logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG)
 
'''KVIndex is the Baseclass for all Backend Adapters. it needs initialization via __init__  from the subclass!'''
class KVIndex(object):

 
    def __init__(self, opts, name, reorder=True):
        self.internal_ordering = name
        self.input_ordering = opts.get("database", "naturals")
        self.keylength = int(opts.get("index", "keylength"))
        self.logger = logging.getLogger("tygrstore.index") 
        self.updateable = self.config_file.get("general","updateable")    
        if reorder: self.setup_reordering_decorators()
        
    
    '''here we change the input and output of 
    add_triple  (input)
    count (input)
    ids_for_triple (input, output) to match the order of the index
    ''' 
    def setup_reordering_decorators(self):
        self.reordering = []
        for i in self.internal_ordering:
            self.reordering.append(self.input_ordering.find(i))         
        self.logger.info("reordering: %s" % str(self.reordering))      
        self.ids_for_triple = self.input_reorder_wrapper(self.ids_for_triple) 
        self.add_triple = self.input_reorder_wrapper(self.add_triple)
        self.selectivity_for_triple = self.input_reorder_wrapper(self.selectivity_for_triple)
        
         
    def input_reorder_wrapper(self, original_func):
        def reorder(the_tuple, **kwargs):
            try:
                reordered_tuple = []
                for x in self.reordering:
                    reordered_tuple.append(the_tuple[x])
                reordered_tuple = tuple(reordered_tuple)           
                return original_func(reordered_tuple, **kwargs)     
            except IndexError: 
                self.logger.error("defect tuple!")
                self.logger.error( the_tuple  )            
                #import pdb; pdb.set_trace()       
        return reorder       
     
                       


        
        