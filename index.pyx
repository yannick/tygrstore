# cython: profile=True
INDEX_OPEN = 42
#todo: 
INDEX_CLOSED = 666  
import binascii
import logging
#LOG_FILENAME = 'index.log'
#logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG)

class KVIndex(object):

 
    def __init__(self, opts, name):
        self.internal_ordering = name
        self.input_ordering = opts.get("database", "naturals")
        self.keylength = int(opts.get("index", "keylength"))
        self.logger = logging.getLogger("tygrstore.index") 
        self.updateable = self.config_file.get("general","updateable")    
        self.setup_reordering_decorators()
        
    
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
     
                       

import redis        
class KVIndexRedis(KVIndex):
    def __init__(self, name="spo", host='localhost', port=6379, path="", keylength=20):
        self.is_open = INDEX_CLOSED 
        self.name = name
        self.internal_ordering = name
        self.input_ordering = "spo"  
        # hmm: http://fuhm.net/super-harmful/
        super(KVIndexRedis, self).__init__(keylength=keylength) 
        self.host = host
        self.port = port
        self.shared = False         
        self.levels = [] #todo use list
        
        self.key_prefix = name
        self.open()
    
    '''deletes all levels of the database'''    
    def delete(self):
        for r in self.levels:
            r.flushdb()
            

    def open(self):        
        self.levels.append(redis.Redis(self.host, self.port, db=10) )
        self.levels.append(redis.Redis(self.host, self.port, db=11) )
        self.is_open = INDEX_OPEN    

    def close(self):
        pass
    
    def add_triple(self, triple):
        '''adds a triple. 
        
        with redis its not necessary to check for duplicates since that is done automatically
        '''
        sid,pid,oid = triple 
        
        #add the pid to the level0 set
        self.levels[0].sadd(":".join( [ self.key_prefix, sid]), pid)
        
        # : is a convention in redisworld 
        #add oid to level1 set: eg for (22,33,44) the key is spo:l1:33:44
        self.levels[1].sadd(":".join( [ self.key_prefix, sid,pid]), oid) 
      
    def __len__(self):
        #we can use dbsize since we use a different db for each index.
        #otherwise we would need to keep track of the count
        return self.levels[1].dbsize() 
        
        
    def selectivity_for_triple(self, triple):
        sid,pid,oid = triple

        if (sid,pid,oid) == (None,None,None):
            return len(self)
        # (?x, ?y, None)
        if sid is not None and pid is not None and oid is None:
            return self.levels[1].scard( ":".join( [ self.key_prefix, sid,pid]) ) #key is "prefix:sid:pid"
        # (?x, None, None)             
        elif sid is not None and pid is None and oid is None:
            return self.levels[0].scard( ":".join( [ self.key_prefix, sid]) )
        else:
            raise NotImplementedError("you tried to count something weird")   

    #generator for ids
    #this implementation will not scale well!
    #we need to change the smembers function to be a generator for ids! 
    def ids_for_triple(self,triple):
        sid,pid,oid = triple
       
        # (?x, ?y, None)
        if sid is not None and pid is not None and oid is None:
            for key in self.levels[1].smembers( ":".join( [ self.key_prefix, sid,pid])):
                yield key
        # (?x, None, None)
        elif sid is not None and pid is None and oid is None:
            #get all pid's
            for pid in self.levels[0].smembers( ":".join( [ self.key_prefix, sid])):
                for oid in self.levels[1].smembers( ":".join( [ self.key_prefix, sid,pid])):
                    yield oid 
        else:
            raise NotImplementedError("")
                    
                                

        
        