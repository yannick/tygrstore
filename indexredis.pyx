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
# 
import redis        
class KVIndexRedis(KVIndex):
    def __init__(self, config, name="spo" ): 
        self.config = config 
        self.host=self.config_file.get("mongodb", "host") 
        self.port=self.config_file.getint("mongodb", "port") 
        self.keylength= self.config_file.get("mongodb", "keylength") 
        
        self.is_open = INDEX_CLOSED 
        self.name = name
        self.internal_ordering = name
        self.input_ordering = "spo"  
        # hmm: http://fuhm.net/super-harmful/
        super(KVIndexRedis, self).__init__(keylength=keylength) 

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
                    
                                
