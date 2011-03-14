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
    naturals = "spo"
    outfile = open(options.outfile, "w")
    s0 = naturals.index(options.ordering[0])
    s1 = naturals.index(options.ordering[1])
    s2 = naturals.index(options.ordering[2])
    totalkeys = options.totalkeys
    tmp = time.time()
    st = time.time()
    cnt =0
    for line in sys.stdin:
        cnt +=1
        if (cnt % 500000) == 0:
            tkps = 500000/(time.time() - tmp)
            kps = cnt/(time.time() - st) 
            now = time.time() - st   
            est = (totalkeys - cnt)/tkps
            print "key %s  at %f kps. overall: %f kps, total elapsed: %02f min. estimated: %f h" % (cnt, tkps, kps, now/60, est/3600) 
            tmp = time.time()  
        triple = triplematch.findall(line)[0]
        outfile.write("%s%s%s\n" % (hl.md5(triple[s0]).hexdigest(),hl.md5(triple[s1]).hexdigest(),hl.md5(triple[s2]).hexdigest()))
    outfile.close()
    
def main(): 
    
    usage = "usage: %prog [options] SOURCE"
    parser = OptionParser(usage=usage)
    # parser.add_option('-a','--hash_algorithm', type='string',
    #                   action='store', dest='hash_function', default='md5',
    #                   help='''the hashing function. possible values: sha1, md5''') 
    # parser.add_option('-l','--key_length', type='long',
    #                    action='store', dest='key_length', default=16,
    #                    help='the key length in bytes')     
    parser.add_option('-o','--ordering', type='string',
                   action='store', dest='ordering', default="spo",
                   help='the desired ordering of the output')
    parser.add_option('-f','--outfile', type='string',
                    action='store', dest='outfile', default="spo",
                    help='the name of the outfile')
    parser.add_option('-t','--totalkeys', type='int',
                    action='store', dest='totalkeys', default="1300000",
                    help='how many triples')                                         
                                                      
    (options, args) = parser.parse_args()
#    if len(args) != 3:
#        parser.error("incorrect number of arguments (perhaps you did not specify the SOURCE, use --help for further details)")
    #hashfunction = get_hash_function(options.hash_function, options.key_length)
    
    t0      = time.time()
    process( options)
    print 'Took %s seconds'%(str(time.time()-t0))        


if __name__ == '__main__':
    main()