# cython: profile=True
import os                         
import kyotocabinet as kc 
import binascii
import logging
from index import *
from helpers import *   

'''represents a Kyoto Cabinet backed spo-type index'''
class KVIndexKC(KVIndex):
   
    def __init__(self,config_file, name="spo"): 
        self.is_open = INDEX_CLOSED
        self.config_file = config_file          
        self.path = os.path.abspath(self.config_file.get("database", "path")) 
        self.db_config = config_file.get("kc", "indexconfig")       
        
        # hmm http://fuhm.net/super-harmful/
        #setup_reordering_decorators and set keylength
        super(KVIndexKC, self).__init__(self.config_file,name)                
        self.levels = []       
        self.filename_prefix = name
        self.is_open = self.open()
        self.shared = False
     
# tygrstore api methods
    def open(self):
        if self.is_open == INDEX_OPEN:
            return INDEX_OPEN
        for lvl in range(0,len(self.internal_ordering)):
             bdb = kc.DB()                           
             fullpath = os.path.join(self.path, "%s%s.kct%s" % (self.filename_prefix, str(lvl),  self.db_config )) 
             self.logger.debug("opening: " + os.path.join(self.path, "%s%s.kct" % (self.filename_prefix, str(lvl))))  
             #open all indexes according to the config either read only or updateable
             if eval(self.updateable):
                 rw_opts = kc.DB.OCREATE | kc.DB.OWRITER
             else:
                 rw_opts = kc.DB.OREADER
                 
             if not bdb.open(fullpath, rw_opts):
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
        #sid,pid,oid = triple   
        full_key = "".join(triple)
        #check if its already in there   
        if self.levels[-1].get(full_key):            
            self.logger.debug("triple/quad already in the store: %s" % str(triple)) 
        else:
            try:                                                          
                #self.logger.debug("trying to set %s" % binascii.hexlify(full_key))
                self.levels[-1].set(full_key, "") 
                for i in range(0,len(self.levels)-1):             
                    key = full_key[:((i+1)*self.keylength)]   
                    self.levels[i].increment(key,1) 
                #self.levels[1].increment("".join([sid,pid]), 1) 
                #self.levels[0].increment(sid, 1)
            except Exception, e:
                self.logger.error("inserting triple/quad failed! %s" % str(triple)) 
                print e 
            
        
    def __len__(self):
        return self.levels[-1].count() 
     

    '''get the selectivity count'''    
    def selectivity_for_triple(self, triple):
        if triple == (None,) * len(triple):
            return len(self)
        else:
            triple_without_none = filter(lambda x: x != None, triple)
            return self.levels[len(triple_without_none)-1].increment( "".join(triple_without_none),0)            
                            
                          
    def ids_for_triple(self,triple, num_records=-1, searched_natural=""):
        #sid,pid,oid = triple
        triple_without_none = filter(lambda x: x != None, triple) 
        left_offset = len(triple_without_none) * self.keylength
        return self.generator_for_searchstring_with_jump("".join(triple_without_none),loffset=left_offset,roffset=left_offset+self.keylength, num_records=num_records)
        
        
#generators    
        

    def generator_for_searchstring_with_jump(self,searchstring,loffset=0,roffset=16, num_records=0):  
        #select the deepest level 
        cur = self.levels[-1].cursor()
        #jump to the lowest possible key 
        cur.jump(searchstring)                
        while True:
            try:
                next = cur.next() 
                #check if we are still within the correct keyspace
                if next[:loffset] == (searchstring):   
                    #yield the key and receive the next possible lowest key (jumpto)
                    jumpto = yield(next[loffset:roffset])                   
                    if jumpto:   
                        #advance the cursor
                        cur.jump("".join((searchstring, jumpto)))                 
                else:
                    #keyspace is exhausted  
                    raise StopIteration                                           
            except KeyError:
                self.logger.error("key error for: %s" % str(searchstring)) 
                cur = self.levels[-1].cursor()
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