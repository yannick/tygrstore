INDEX_OPEN = 42
INDEX_CLOSED = 666
class KVIndex(object):

 
    def __init__(self, keylength=20):
        
        self.keylength = keylength
        setup_reordering_decorators(self.ordering)
    
    '''here we change the input and output of 
    add_triple  (input)
    count (input)
    ids_for_triple (input, output) to match the order of the index
    ''' 
    def setup_reordering_decorators(self):
        self.reordering = []
        for i in self.internal_ordering:
            self.reordering.append(self.input_ordering.find(i))         
        self.original_method = self.reorder_wrapper(self.original_method)
        
    def reorder_wrapper(self, original_func):
        def reorder(the_tuple):
            reordered_tuple = tuple(the_tuple[x] for x in self.reordering)           
            reordered_tuple = original_func(reordered_tuple)         
            the_tuple = tuple(reordered_tuple[x] for x in self.reordering)
            return the_tuple
        return reorder     
     
                       

import redis        
class KVIndexRedis(KVIndex):
    def __init__(self, name="spo", host='localhost', port=6379, path="", keylength=20):
        self.is_open = INDEX_CLOSED 
        self.name = name
        self.ordering = name
        super(KVIndexRedis, self).__init__(keylength=keylength) 
        self.host = host
        self.port = port
        self.shared = False         
        self.levels = [] #todo use list
        
        self.key_prefix = name
        self.open()
        
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
        
        
    def count(self, triple):
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
                    
                                
import os                         
from tc import *
'''represents a Tokyo Cabinet backed spo-type index'''
class KVIndexTC(KVIndex):
    
    def __init__(self, name="spo", path=".", keylength=20 ):
        self.name = name  
        self.ordering = name
        super(KVIndexTC, self).__init__(keylength=keylength, ordering=name)
        self.path = os.path.abspath(path)
        self.is_open = INDEX_CLOSED
        self.levels = [] #todo use list
        
        self.filename_prefix = name
        self.is_open = self.open()
        self.shared = False
     
# tygrstore api methods
    def open(self):
        if self.is_open == INDEX_OPEN:
            return INDEX_OPEN
        for i in range(0,3):
             bdb = BDB() 
             #tuning?             
             fullpath = os.path.join(self.path, "%s%s.bdb" % (self.filename_prefix, str(i)))
             #print fullpath
             bdb.open(fullpath, BDBOWRITER | BDBOREADER | BDBOCREAT)
             self.levels.append(bdb)          
        self.is_open = INDEX_OPEN
        return self.is_open  



    def close(self):
        if self.is_open == INDEX_CLOSED:
            return INDEX_CLOSED       
        for f in self.levels:
            f.close()
        self.is_open = INDEX_CLOSED
        return INDEX_CLOSED

    def delete(self):
        '''deletes all databases'''
        close = self.close()
        for i in range(0,3):
            filen = os.path.join(self.path, "%s%s.bdb" % (self.filename_prefix, str(i)))
            #unlink/delete not working, why??? 
            os.unlink( filen )
            
        
    def add_triple(self, triple):
        sid,pid,oid = triple   
        full_key = "".join(triple)
        #check if its already in there   
        if self.levels[2].has_key(full_key):
            self.levels[0].addint(sid, 1) 
            self.levels[1].addint("".join([sid,pid]), 1)
            self.levels[2].put(full_key, "")  
          
        
    def __len__(self):
        return len(self.levels[2]) 
        
    def count(self, triple):
        sid,pid,oid = triple
        if (sid,pid,oid) == (None,None,None):
            return len(self)
        if sid is not None and pid is not None and oid is None:
            if self.shared: #todo decorator
                #get keys from level1 and then search level2
                raise NotImplementedError("")
            else:
                return self.levels[1].addint( "".join([sid,pid]) , 0)             
        elif sid is not None and pid is None and oid is None:
            return self.levels[0].addint(sid, 0)
        else:
            raise NotImplementedError("you tried to count something weird")    
             

                
                
                          
    def ids_for_triple(self,triple):
        sid,pid,oid = triple
        if sid is not None and pid is not None and oid is None:
            searchstring = "".join([sid,pid])               
            return self.generator_for_searchstring(searchstring)
            #search in level2
        elif sid is not None and pid is None and oid is None:
            searchstring = sid
            
            if self.shared:
                #get keys from level1 and then search level2
                raise NotImplemented("")
            else:
                return self.generator_for_searchstring(searchstring)
        else:
            raise NotImplementedError("")                
           
        
        
#generators    

    def generator_for_searchstring(self,searchstring):
        #print searchstring
        cur = self.levels[2].curnew()
        cur.jump(searchstring) 
        #TODO: cleanup while 1, WTF???   
        while 1:
            next = cur.next() 
            # todo: speedup
            if next.startswith(searchstring):
                yield next
            else: 
                raise StopIteration         
                
    #untested
    def chunk_generator_for_searchstring(self,searchstring, chunksize):
        cur = self.levels[2].curnew()
        cur.jump(searchstring)
        while 1:
            next = cur.next() #why cant that be in the while clause... 
            list_of_ids = []
            top = chunksize
            while top > 0:
                top -= 1 
                if next.startswith(searchstring):
                    list_of_ids.append(next)
                    
                else:
                    if len(list_of_ids) > 0:
                        yield list_of_ids
                    else: 
                        raise StopIteration
        yield list_of_ids   
                                    
        
   
                                           
    
                           
        
        
        
        
        
        
        
        
        
        
        
        
        