import os                         
import kyotocabinet as kc 
import binascii
import logging
from index import *

'''represents a Kyot Cabinet backed spo-type index'''
class KVIndexTC(KVIndex):
    #TODO: add quad support
    
    def __init__(self, name="spo", path="./data_lubm/", keylength=16 ): 
        self.keylength = keylength
        self.name = name  
        self.internal_ordering = name
        self.input_ordering = "spo"
        # hmm http://fuhm.net/super-harmful/
        super(KVIndexTC, self).__init__(keylength=keylength)
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
        for lvl in range(0,3):
             bdb = kc.DB() 
             #tuning?
                           
             fullpath = os.path.join(self.path, "%s%s.kct" % (self.filename_prefix, str(lvl))) 
             if not bdb.open(fullpath, kc.DB.OREADER):
                 raise BaseException("could not open %s" % fullpath)
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
        if (close != INDEX_CLOSED):
            raise BaseException("could not close" )
        for i in range(0,3):
            filen = os.path.join(self.path, "%s%s.bdb" % (self.filename_prefix, str(i)))
            #unlink/delete not working, why??? 
            os.unlink( filen )
            
        
    def add_triple(self, triple):
        sid,pid,oid = triple   
        full_key = "".join(triple)
        #check if its already in there   
        if self.levels[2].get(full_key):            
            self.logger.debug("triple already in the store: %s" % str(triple)) 
        else:
            self.levels[2].set(full_key, "") 
            self.levels[1].increment("".join([sid,pid]), 1) 
            self.levels[0].increment(sid, 1) 
            
        
    def __len__(self):
        return self.levels[2].count() 
    
    '''get the selectivity count'''    
    def count(self, triple):
        sid,pid,oid = triple
        if (sid,pid,oid) == (None,None,None):
            return len(self)
        if sid is not None and pid is not None and oid is None:
            if self.shared: #todo decorator
                #get keys from level1 and then search level2
                raise NotImplementedError("")
            else:
                return self.levels[1].increment( "".join([sid,pid]) , 0)             
        elif sid is not None and pid is None and oid is None:
            return self.levels[0].increment(sid, 0)
        else:
            raise NotImplementedError("you tried to count something weird")    
             

                
                
                          
    def ids_for_triple(self,triple, num_records=-1):
        sid,pid,oid = triple
        #subject and predicate given
        if sid is not None and pid is not None and oid is None:
            searchstring = "".join([sid,pid])               
            return self.generator_for_searchstring_with_jump(searchstring,loffset=32,roffset=48, num_records=num_records)
            #search in level2 
        #only subject given
        elif sid is not None and pid is None and oid is None:
            searchstring = sid
            
            if self.shared:
                #get keys from level1 and then search level2
                raise NotImplemented("")
            else:
                return self.generator_for_searchstring_with_jump(searchstring,loffset=16,roffset=32, num_records=num_records)
        else:
            raise NotImplementedError("")                
           
        
        
#generators    
        

    def generator_for_searchstring_with_jump(self,searchstring,loffset=0,roffset=16, num_records=0):
        #print searchstring
        cur = self.levels[2].cursor() 
        cur.jump(searchstring)        
        while 1:
            try:
                next = cur.next()
                if next[0:loffset] == (searchstring):
                    jumpto = yield(next[loffset:roffset])
                    if jumpto:
                        cur.jump("".join((searchstring, jumpto)))
                else: 
                    raise StopIteration                                           
            except KeyError:
                #print "KeyError"
                cur = self.levels[2].cursor()
                cur.jump(next)
                         
                                             
  
                                                    
    #untested, POC
    def chunk_generator_for_searchstring(self,searchstring, chunksize):
        cur = self.levels[2].cursor()
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