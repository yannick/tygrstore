#!/usr/bin/env python2
import ConfigParser
from optparse import OptionParser

import time 
import sys
import re

import kyotocabinet as kc
import tc
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

def process(options):
    db = kc.DB() 
    dbstring = options.outfile + "#bnum=" + options.bnum + "#msiz=4G#pccap=4G#apow=8#rcnum=0"
    db.open( dbstring, kc.DB.OWRITER | kc.DB.OCREATE )

    totalkeys = options.totalkeys 
    #totalkeys = 13409690 
    cache = {}
    cnt = 0 
    tmp = time.time()
    st = time.time()
    cache = {}
    for line in sys.stdin:
        cnt +=1
        if (cnt % options.cache) == 0:
            tkps =  options.cache/(time.time() - tmp)
            kps = cnt/(time.time() - st) 
            now = time.time() - st   
            est = (totalkeys - cnt)/tkps
            print "key %s  at %f kps. overall: %f kps, total elapsed: %02f min. estimated: %f h" % (cnt, tkps, kps, now/60, est/3600) 
            tmp = time.time()  
    
            sti = time.time()
            for k,v in cache.iteritems():
                db.set(k,v)
            endt = time.time() - sti
            cache = {}
            print "inserting into tree took %f seconds, that is %f ktps" % (endt,  options.cache/endt)
        (s,p,o) = triplematch.findall(line)[0]
        #collection.insert( { "s":Binary(hl.md5(trip[0]).digest),"p":Binary(hl.md5(trip[1]).digest),"o":Binary(hl.md5(trip[2]).digest) } )
        cache[hl.md5(s).digest()] = s
        cache[hl.md5(p).digest()] = p
        cache[hl.md5(o).digest()] = o 
    print "final round!"
    for k,v in cache.iteritems():
        db.set(k,v)        
    db.close()                      
 

def main(): 

    usage = "makes a stringstore hash usage: %prog [options] SOURCE"
    parser = OptionParser(usage=usage)
    # parser.add_option('-a','--hash_algorithm', type='string',
    #                   action='store', dest='hash_function', default='md5',
    #                   help='''the hashing function. possible values: sha1, md5''') 
    # parser.add_option('-l','--key_length', type='long',
    #                    action='store', dest='key_length', default=16,
    #                    help='the key length in bytes')     
    parser.add_option('-b','--bnum', type='string',
                   action='store', dest='bnum', default="spo",
                   help='the desired ordering of the output')
    parser.add_option('-f','--outfile', type='string',
                    action='store', dest='outfile', default="spo",
                    help='the name of the outfile')
    parser.add_option('-t','--totalkeys', type='int',
                    action='store', dest='totalkeys', default=138318414,
                    help='how many triples')                                         
    parser.add_option('-c','--cachesize', type='int',
                    action='store', dest='cache', default=138318414,
                    help='how many triples are cached')  
                                      
    (options, args) = parser.parse_args()

    t0      = time.time()
    process( options)
    print 'Took %s seconds'%(str(time.time()-t0))        


if __name__ == '__main__':
    main()   
