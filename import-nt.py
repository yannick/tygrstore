#!/usr/bin/env python
import sys, time
import os
import binascii as ba
import re
import hashlib 
import hashlib as hl  
import ConfigParser
from optparse import OptionParser
from itertools import permutations
import gzip 
import kyotocabinet as kc
import heapq 

uri = r'<[^:]+:[^\s"<>]+>' 
langtag = r'@[a-z]+-[A-Za-z0-9]+'
dtype =          r'\^{2}' + uri
literal =  r'"[^"\\]*(?:\\.[^"\\]*)*"'
literalplus = literal + r'(?:' + langtag + r'|' + dtype + r')?'
wholeline = r'(' + uri + r')\s' + r'(' +  uri + r')\s' + r'(' + uri + "|" + literalplus + r')\s.\n'

triplematch = re.compile(wholeline)     


def get_hash_function(function, keylength):
    def makesha1(string):
        return hashlib.sha1(string).digest()[0:keylength]  
    def makemd5(string):
        return hashlib.md5(string).digest()[0:keylength]
    hashing_functions = {"sha1": makesha1, "md5": makemd5} 
    return hashing_functions[function]

def flush_rbtree(tree, dirpath, filenumber):
    print "flushing to file %i" % filenumber 
    path = os.path.join(dirpath, "s2id." + str(filenumber) + ".gz")
    f = gzip.open(path, "w")
    for key in tree:
        f.write(key + " " + tree[key] + "\n")
    f.close()
    print "...finished"
    
def process(hashf, options): 
    dirpath = options.path
    inmemory_size = options.inmemory_size 
    s2id_config = '#bnum=1000k#psiz=65536#pccap=32m#opts=c'
       
    trees = {}
    for p in permutations("spo"):
        #db = kc.DB()
        #filename = os.path.join(dirpath, p + ".kct"
        #db.open('%#bnum=1000k#psiz=65536#pccap=32m#opts=c', kc.DB.OWRITER | kc.DB.OCREATE)
        #db.open(filename + '#bnum=1000k#psiz=65536#pccap=32m#opts=c', kc.DB.OWRITER | kc.DB.OCREATE) 
        #trees[p] = db
        trees[p] = []
        
    s2id_tree = kc.DB()  #rbtree.rbtree() 
    filename = os.path.join(dirpath, "id2s.0.kct")
    s2id_tree.open(filename + s2id_config, kc.DB.OWRITER | kc.DB.OCREATE)
    #s2id_tree.open('%#bnum=1000k#psiz=65536#pccap=32m#opts=c', kc.DB.OWRITER | kc.DB.OCREATE)        

    (subj,pred,obje) = (s,p,o) = ("","","")    
    start = time.time() 
    
    counter = 0
    filenumber = 0  

    reporting_step = inmemory_size / 10
    
    start_time = time.time()
    round_time = time.time()
 
    try:
        for line in sys.stdin:

            try:   
                [(subj,pred,obje)] = triplematch.findall(line)
            except:
                print "could not parse line: %s" % line
                
            #print "%i %i" % (counter, inmemory_size)   
            if (counter % reporting_step == 0):
                print "%i (importing %f triples per second)" % (counter, (reporting_step / (time.time() - round_time)))
                round_time = time.time()   
                 
            if (counter >= inmemory_size):
                #write temporary files
                print "adding %i keys took %f seconds. avg %f /second" % (inmemory_size, time.time() - start_time, inmemory_size / (time.time() - start_time) )                                                
                print "now flushing indexes"
                start_flushing = time.time()
                for index_name,dbhandle in trees.iteritems():
                    fname = "".join(index_name) + "." + str(options.key_length) + "." + str(filenumber) + ".hxidx.kct"
                    full_fname = os.path.join(dirpath, fname)
                    sys.stdout.write("writing file " + full_fname) 
                    #f = open(full_fname, 'bw')
                    out_db = kc.DB()
                    out_db.open(full_fname + '#bnum=1000k#psiz=65536#pccap=32m#opts=c', kc.DB.OWRITER | kc.DB.OCREATE) 
                    sys.stdout.flush()
                    for i in range(len(dbhandle)): 
                        if (i % (inmemory_size/10) == 0):
                            sys.stdout.write(".")
                            sys.stdout.flush()
                        out_db.set( heapq.heappop(dbhandle), '')
                    out_db.close()
                    print "closed & flushed."                     
                    #print fname + " closed and db cleared"  
                
                
                 
                flush_time = time.time() - start_flushing
                print "flushing took %i seconds. makes totaltime of %f    overal avg that is %f triples per second" % ( flush_time, time.time() - start_time, inmemory_size / (time.time() - start_time)  )
                    
                filenumber += 1 
                
                
                sys.stdout.write("flushing s2id") 
                sys.stdout.flush() 
                s2id_tree.close()
                del(s2id_tree)
                s2id_tree = kc.DB()  #rbtree.rbtree() 
                filename = os.path.join(dirpath, "id2s." + str(filenumber) + ".kct")
                s2id_tree.open(filename + s2id_config, kc.DB.OWRITER | kc.DB.OCREATE)
                print "done"
                
                counter = 0                      
                start_time = time.time() 
            
           
            counter += 1
            s,p,o = hashf(subj),hashf(pred), hashf(obje)
            
            s2id_tree[s] = subj
            s2id_tree[p] = pred
            s2id_tree[o] = obje

            
            for index_name,filehandle in trees.iteritems():
                key = "".join( ( locals()[index_name[0]], locals()[index_name[1]], locals()[index_name[2]] ))  
                heapq.heappush(filehandle, key )
                

               
        s2id_tree.close()
        #flush_rbtree(s2id_tree, dirpath, filenumber) 
    
    except KeyboardInterrupt:
      print "interupted"
      #flush_rbtree(s2id_tree, dirpath, filenumber)    
        #print "%s %s %s" % (s,p,o)
    #s2id_tree.close()        
    print 'reading lines took %s seconds' % (str(time.time()-start)) 

def main(): 
    
    usage = "usage: %prog [options] SOURCE"
    parser = OptionParser(usage=usage)
    parser.add_option('-a','--hash_algorithm', type='string',
                      action='store', dest='hash_function', default='md5',
                      help='''the hashing function. possible values: sha1, md5''')
    parser.add_option('-l','--key_length', type='long',
                      action='store', dest='key_length', default=16,
                      help='the key length in bytes')
    parser.add_option('-p','--path', type='string',
                   action='store', dest='path', default=".",
                   help='the key length in bytes')
    parser.add_option('-s','--size', type='long',
                    action='store', dest='inmemory_size', default=1000000,
                    help='the number of keys that should be cached until the id2string file is flushed to disk')                         
                                           
    (options, args) = parser.parse_args()
#    if len(args) != 3:
#        parser.error("incorrect number of arguments (perhaps you did not specify the SOURCE, use --help for further details)")
    hashfunction = get_hash_function(options.hash_function, options.key_length)
    
    t0      = time.time()
    process(hashfunction, options)
    print 'Took %s seconds'%(str(time.time()-t0))        


if __name__ == '__main__':
    main()