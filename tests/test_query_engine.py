#nosetests
from index import *
import os
from stringstore import *
from query_engine import *

class Test_Tygrstore(object):


    def setUp(self):
        self.spo_index_filename = "db/test_index.bdb"
        self.spo_index = KVIndexRedis(name="spo", path=".")
        status = self.spo_index.is_open
        assert(status == INDEX_OPEN, "could not open index")
        assert(self.spo_index.name == "spo")

    def tearDown(self):
        "tear down test fixtures"
        
    def test_qe(self):
        sstore = Stringstore()                            
        qu = QueryEngine()
        qry = file("tests/sparql1.query").read()
        qu.stringstore = sstore 
        qu.execute(qry)