                          #nosetests
from stringstore import *
import os

class Test_Stringstore(object):
    
    def setUp(self):
        self.stringstore = Stringstore()
        
    def tearDown(self):
        "tear down test fixtures"
        pass   
            
    def test_crud(self):
        pass