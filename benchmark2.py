#!/usr/bin/env python
import ConfigParser
import pstats, cProfile
import hashlib as hl
import os
import time                          
from kyoto_cabinet_stringstore import *
from index_manager import *
from query_engine import * 
import logging 
from lubm_queries2 import *
import stopwatch


LOG_FILENAME = 'logs/benchmark1-kc-kc.log'
logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG)   


config = ConfigParser.RawConfigParser()
config.read("cfgs/benchmark1-kc-kc.cfg") 

logging.debug("starting stringstore")
stringstore = KyotoCabinetStringstore(config)


logging.debug("starting index_manager")
index_manager = IndexManager(config)   

def c():
    stringstore.close()
    index_manager.close()
                        

logging.debug("starting QueryEngine")  

qe = QueryEngine(stringstore, index_manager, config)   

bqry = prefix+queries["lq5"]

def testrun():  
    cnt = 0
    for i in islice(qe.execute(bqry),results):
        cnt += 1
    print "got %i results" % cnt
    

results = 10000    
cProfile.runctx("testrun()", globals(), locals(), "benchmark2.prof")
s = pstats.Stats("benchmark2.prof")
s.strip_dirs().sort_stats("time").print_stats()
   

# def testrunhs():
#     cnt = 0
#     for i in qe.execute(bqry):
#         cnt += 1
#     print "got %i results" % cnt
#     return "ok"
#     
# import hotshot, hotshot.stats, test.pystone    
# hsfile = "benchmark1.hotshot.prof"
# prof = hotshot.Profile(hsfile)
# benchtime, stones = prof.runcall(testrunhs) 
# print "closing"
# prof.close()  
# "print opening"
# stats = hotshot.stats.load(hsfile)     
# stats.strip_dirs()
# stats.sort_stats('time', 'calls')
# stats.print_stats(50)                                  


#c()              
