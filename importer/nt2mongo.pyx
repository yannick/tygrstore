#!/usr/bin/env python2
import time 
import sys
import re

import pymongo
import bson
from bson import ObjectId
from bson.binary import Binary
import hashlib as hl
import gzip

uri = r'<[^:]+:[^\s"<>]+>' 
langtag = r'@[a-z]+-[A-Za-z0-9]+'
dtype =          r'\^{2}' + uri  
bnode = r'_:[A-Za-z][A-Za-z0-9]*'

literal =  r'"[^"\\]*(?:\\.[^"\\]*)*"'
literalplus = literal + r'(?:' + langtag + r'|' + dtype + r')?'
wholeline = r'(' + uri + "|" + bnode + r')\s' + r'(' +  uri + r')\s' + r'(' + uri + "|" + literalplus + "|" + bnode + r')\s.\n' 

triplematch = re.compile(wholeline)    


conn = pymongo.Connection("192.168.111.117")
db = conn["tygrstore"]
collection = db["spo"]

lubm100 = "/Volumes/data/databases/lubm-sorted.nt"
lubm1k = "/Volumes/linked_data/lubm1000.nt"
lubm1kgz = "/Volumes/data/databases/lubm1000.nt.tar.gz"
infile = open(lubm1k) #gzip.open(lubm1kgz)#open(lubm1k) 

totalkeys = 138318414
#totalkeys = 13409690
cnt = 0 
tmp = time.time()
st = time.time()
for line in infile:
    cnt +=1
    if (cnt % 10000) == 0:
        tkps = 10000/(time.time() - tmp)
        kps = cnt/(time.time() - st) 
        now = time.time() - st   
        est = totalkeys/tkps
        print "key %s  at %f kps. overall: %f kps, total elapsed: %s sec. estimated: %f sec" % (cnt, tkps, kps, now, est) 
        tmp = time.time()
    trip = triplematch.findall(line)[0]
    #collection.insert( { "s":Binary(hl.md5(trip[0]).digest),"p":Binary(hl.md5(trip[1]).digest),"o":Binary(hl.md5(trip[2]).digest) } )
    x = { "s":Binary(hl.md5(trip[0]).digest()),"p":Binary(hl.md5(trip[1]).digest()),"o":Binary(hl.md5(trip[2]).digest()) }
    collection.insert(x)