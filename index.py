INDEX_OPEN = 42
INDEX_CLOSED = 666
class KVIndex(object):

 
    def __init__(object):
        pass
        
    #type of id

    #count_of_all records

    #generator for list of id's

    #curser from an index given the prefix,

    #            
    def get(self):
        pass
    
    def create(self):
        pass
   
    def open(self):
        pass
     
    def close(self):
        pass

    def count(self, triple):
        pass
    
    def delete(self):
        "harakiri"
    
    def __len__(self):
        return 42 
        
   

import redis        
class KVIndexRedis(KVIndex):
    def __init__(object, redis, name="spo"):
        pass


    def get(self):
        pass

    def create(self):
        pass

    def open(self):
        pass

    def close(self):
        pass

   
import os                         
from tc import *
'''represents a spo-type index'''
class KVIndexTC(KVIndex):
    def __init__(self, name="spo", path="." ):
        self.path = os.path.abspath(path)
        self.is_open = INDEX_CLOSED
        self.levels = []
        self.name = name
        self.filename_prefix = name
        is_open = self.open()
        self.shared = False
     
# tygrstore api methods
    def open(self):
        if self.is_open == INDEX_OPEN:
            return INDEX_OPEN
        for i in range(0,3):
             bdb = BDB()              
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
        #print "inserting %s %s %s" % triple
        s,p,o = triple
  
        self.levels[0].addint(self.id_to_key(s), 1) 
        self.levels[1].addint(self.id_to_key(s) + self.id_to_key(p), 1)
        self.levels[2].put(self.triple_to_key(triple), "") 
          
        
    def __len__(self):
        return len(self.levels[2].keys()) 
        
    def count(self, triple):
        (s,p,o) = triple
        if (s,p,o) == (None,None,None):
            return len(self)
        if s is not None and p is not None and o is None:
            if self.shared:
                #get keys from level1 and then search level2
                raise NotImplemented("")
            else:
                return self.levels[1].addint(self.id_to_key(s) + self.id_to_key(p), 0)             
        elif s is not None and p is None and o is None:
            return self.levels[0].addint(self.id_to_key(s), 0)
        else:
            raise NotImplemented("you tried to count something weird")    
             

                
                
                          
    def ids_for_triple(self,triple):
        s,p,o = triple
        if s is not None and p is not None and o is None:
            searchstring = self.id_to_key(s) + self.id_to_key(p)               
            return self.generator_for_searchstring(searchstring)
            #search in level2
        elif s is not None and p is None and o is None:
            searchstring = self.id_to_key(s)
            
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
        #while 1, WTF???   
        while 1:
            next = cur.next() #why cant that be in the while clause... 
            if next.startswith(searchstring):
                yield next
            else: 
                raise StopIteration 

    def chunk_generator_for_searchstring(self,searchstring, chunksize):
        #print searchstring
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
                                    
        
#helpers        
    def triple_to_key(self, (s,p,o)): 
        '''return a 60 byte string

        >>> triple_to_key((33,44,55)) 
        bytearray(b'000000000000000000330000000000000000004400000000000000000055')
        '''
        #todo optimize
        return self.id_to_key(s) + self.id_to_key(p) + self.id_to_key(o)        
    
    def id_to_key(self,id): 
        return '%0*d' % (20, id)   
                                           
    
                           
        
        
        
        
        
        
        
        
        
        
        
        
        