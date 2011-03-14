import kyotocabinet as kc
import os
from stringstore import Stringstore 
import logging
    
class KyotoCabinetStringstore(Stringstore):
    
    def __init__(self, config_instance):
        self.logger = logging.getLogger('KyotoCabinetStringstore')
        self.logger.debug("init KyotoCabinetStringstore")
        self.config_instance = config_instance                                                    
        self.path = config_instance.get("database", "path")
        self.db_name = config_instance.get("database", "stringstore")
        self.db_config = config_instance.get("kc", "stringstoreconfig")        
        self.logger.debug("init KC Stringstore with cfg file:" + str(config_instance) + " path: " + self.path + " db_name: " + self.db_name)
        super(KyotoCabinetStringstore, self).__init__(config_instance)
        # setting new_id to counter or sha1_hexidgest_for according to the mode
        self.db = kc.DB()
        if eval(config_instance.get("general", "updateable")):
            self.logger.debug("opening %s writeable" % os.path.join(self.path, self.db_name)) 
            self.db.open(os.path.join(self.path, self.db_name + self.db_config), kc.DB.OCREATE | kc.DB.OWRITER)
        else:
            self.logger.debug("opening %s read only" % os.path.join(self.path, self.db_name))
            self.db.open(os.path.join(self.path, self.db_name), kc.DB.OREADER) 
        self.logger.debug("KyotoCabinetStringstore initialized")
    
    def __del__(self):
        self.db.close()
    
    def counter(self):
        pass
           
    '''converts the id to the string'''   
    def id2s(self, an_id):
        return self.db.get(an_id)
    
    '''converts the string to the id'''    
    def s2id(self,a_string):
        if a_string is None: return None
        an_id = self.get_new_id(a_string)
        #if the id is not in the store we raise an exception b/c the query will be invalid
        if (a_string is not None) and (an_id is None):
            raise LookupError("Key not in Store!")
        return an_id
        
    '''returns true if the string is in the store'''
    def contains_string(self,a_string):
        return a_string in self.db
        
                                
    '''adds a string to the store and returns its id'''
    def add_string(self,a_string):                    
        key = self.s2id(a_string)
        if key and self.db.set(key, a_string):
            return key
        else:
            raise KeyError("String could not be added")
    
    def next_id(self):
        pass
         
    def close(self):
        self.db.close()
           
    def __len__():
        return len(self.db)
        
       