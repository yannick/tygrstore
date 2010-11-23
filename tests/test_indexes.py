#nosetests
from index import *
import os

class Test_Tygrstore(object):


    def setUp(self):
        self.spo_index_filename = "test_index.bdb"
        self.spo_index = KVIndexRedis(name="spo", path=".")
        status = self.spo_index.is_open
        assert(status == INDEX_OPEN, "could not open index")
        assert(self.spo_index.name == "spo")

    def tearDown(self):
        "tear down test fixtures"
        
    def test_tc_index(self):
        pass
    
    def get_fresh_index(self, name):
        pass
            
    def test_crud(self):
        spo_index = self.spo_index

#inserting mock data
        mocks = [(1,2,3),
                  (1,2,33),
                  (1,4,3),
                  (1,6,3),
                  (10,6,3) 
                  ]
        for mock in mocks:
            spo_index.add_triple(mock)
        
#total count        
        assert(len(spo_index) == len(mocks), "__len__ of the index is not working correctly" )
        
     
#selectivity 
        assert(spo_index.count( (1,None,None)) == 3, " counting is faulty")
        assert(spo_index.count( (1,2,None)) == 2, " counting is faulty")
        assert(spo_index.count( (10,None,None)) == 1, " counting is faulty")
        assert(spo_index.count( (None,None,None)) == 5, "global counting is faulty") 

#count not in index, what should happen here?        
        #spo_index.count( (None,None,33) )
        
        
#key reading
        l1 = []
        for key in spo_index.ids_for_triple( (1,2,None) ):
            l1.append(key)
        assert(len(l1) == 2)
# test for s only


#triple which the index is not optimized for
#what should happen here?        
#        for key in spo_index.ids_for_triple( (None,None,33) ):
#            l1.append(key)

        
#delete
        ret = spo_index.delete()
        assert(ret == INDEX_CLOSED, "index could not be closed")
        assert(os.listdir(".").count(self.spo_index_filename), "the file %s still exists in %s" % (self.spo_index_filename, os.getcwd()))

                    