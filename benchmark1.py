#!/usr/bin/env python
import ConfigParser
import pstats, cProfile
import hashlib as hl
import os
import time                          
from kyoto_cabinet_stringstore import *
import index_manager as im
import query_engine
import logging 
from index_manager import *
from lubm_queries2 import *
import stopwatch
import cysparql

LOG_FILENAME = 'logs/benchmark1-kc-kc.log'
logging.basicConfig(filename=LOG_FILENAME,level=logging.ERROR)   


config = ConfigParser.RawConfigParser()
config.read("cfgs/benchmark1-kc-kc.cfg") 

logging.debug("starting stringstore")
stringstore = KyotoCabinetStringstore(config)


logging.debug("starting index_manager")
iman = im.IndexManager(config)   



                        

logging.debug("starting QueryEngine")  

qe = query_engine.QueryEngine(stringstore, iman, config) 

def bench(qe):
    for q,sparql in queries.iteritems():
        logging.debug("executing lubm query ")
        nr_of_results = 99999999999999
        print "q & time & no res & 1 res & res/s & qe_setup_time & warm/cold"
        for max_results in [1,10,100,1000,10000, 100000]:
            for run in [ "c", "w"]:
                reload(cysparql)
                reload(im)
                reload(query_engine)
                qe.reload_sparql()
                nr_of_results = 0
                t = stopwatch.Timer()    
                for res in islice(qe.execute(prefix+sparql),max_results):         
                    nr_of_results += 1
                qe_setup_time = qe.stats.stats["qe_setup_time"]
                took = t.elapsed
                print "%s & %f & %i & %f & %f & %f & %s" % (q, took, nr_of_results, took/nr_of_results, nr_of_results/took, qe_setup_time, run )    
                qe = query_engine.QueryEngine(stringstore, iman, config)
                
