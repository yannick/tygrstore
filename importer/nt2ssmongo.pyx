#!/usr/bin/env python2
from multiprocessing import Process, Queue, JoinableQueue, Lock
import time 
import sys
import re

import pymongo
import bson
from bson import ObjectId
from bson.binary import Binary
import hashlib as hl
import gzip
#import kyotocabinet as kc

uri = r'<[^:]+:[^\s"<>]+>' 
langtag = r'@[a-z]+-[A-Za-z0-9]+'
dtype =          r'\^{2}' + uri  
bnode = r'_:[A-Za-z][A-Za-z0-9]*'

literal =  r'"[^"\\]*(?:\\.[^"\\]*)*"'
literalplus = literal + r'(?:' + langtag + r'|' + dtype + r')?'
wholeline = r'(' + uri + "|" + bnode + r')\s' + r'(' +  uri + r')\s' + r'(' + uri + "|" + literalplus + "|" + bnode + r')\s.\n' 

triplematch = re.compile(wholeline)    

host = "192.168.111.117"
conn = pymongo.Connection(host)
db = conn["tygrstore"]
collection = db["id2s"]

lubm100 = "/Volumes/data/databases/lubm-sorted.nt"
lubm1k = "/Volumes/linked_data/lubm1000.nt"
lubm1kgz = "/Volumes/data/databases/lubm1000.nt.tar.gz" 
havoc = "/mnt/linkeddata/lubm1000.nt"
infile = open(lubm1kgz) #gzip.open(lubm1kgz)#open(lubm1k) 

totalkeys = 138318414
#totalkeys = 13409690 
cache = {}
cnt = 0 
tmp = time.time()
st = time.time()
for line in infile:
    cnt +=1
    if (cnt % 5000000) == 0:
        cache = {}
    if (cnt % 10000) == 0:
        tkps = 10000/(time.time() - tmp)
        kps = cnt/(time.time() - st) 
        now = time.time() - st   
        est = totalkeys/tkps
        print "key %s  at %f kps. overall: %f kps, total elapsed: %s sec. estimated: %f sec" % (cnt, tkps, kps, now, est) 
        tmp = time.time()
    trip = triplematch.findall(line)[0]
    #collection.insert( { "s":Binary(hl.md5(trip[0]).digest),"p":Binary(hl.md5(trip[1]).digest),"o":Binary(hl.md5(trip[2]).digest) } )
    for n3 in trip:
        hashv = hl.md5(n3)
        if hashv in cache:
            continue
        else: 
            sh = { "_id":Binary(hashv.digest()), "n3":n3 }
            collection.update(sh,sh, upsert=True)
            cache[hashv] = 1
