#!/usr/bin/env python

import ConfigParser
from optparse import OptionParser     
import time
import re
#from multiprocessing import Process
from multiprocessing import Pool
from itertools import permutations
import tc
import binascii as ba



def main(): 

    usage = '''converts raw indexes to tygrstore databases.
    input dir should contain files 
    '''
    parser = OptionParser(usage=usage)
    parser.add_option('-k','--keylength', type='long',
                      action='store', dest='key_length', default=16,
                      help='the key length in bytes')

    parser.add_option('-o','--output', type='string',
                    action='store', dest='outfile', default="spo",
                    help='the output kc btree file. default: spo')                         
    
    parser.add_option('-i','--input', type='string',
                    action='store', dest='infile', default="spo.sorted.txt",
                    help='input file: spo.sorted.txt') 
    (options, args) = parser.parse_args()

    t0      = time.time()
    
    lvl0 = tc.BDB()
    lvl1 = tc.BDB()
    lvl2 = tc.BDB()
    lvl0.tune(256, 512, 10000000, -1, -1, tc.BDBTLARGE) 
    lvl1.tune(256, 512, 10000000, -1, -1, tc.BDBTLARGE) 
    lvl2.tune(256, 512, 10000000, -1, -1, tc.BDBTLARGE) 
        
    lvl0.open(options.outfile + '0.tcb', tc.BDBOWRITER | tc.BDBOCREAT)
    lvl1.open(options.outfile + '1.tcb', tc.BDBOWRITER | tc.BDBOCREAT)
    lvl2.open(options.outfile + '2.tcb', tc.BDBOWRITER | tc.BDBOCREAT)
    
    
    infile = open(options.infile, 'r')
    
    for line in infile:
        full_key = ba.unhexlify(line.rstrip())
        #if lvl2.get(full_key):            
        #    print "triple already in the store: %s" % str(ba.hexlify(full_key))
        #else:
        lvl2.put(full_key, "") 
        lvl1.addint(full_key[0:(options.key_length*2)], 1) 
        lvl0.addint(full_key[0:(options.key_length)], 1)   
            
    print 'Took %s seconds'%(str(time.time()-t0))        

    
if __name__ == '__main__':
    main()