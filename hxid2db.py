import ConfigParser
from optparse import OptionParser
import tc
import os 
import time
import re
#from multiprocessing import Process
from multiprocessing import Pool
from itertools import permutations
import kyotocabinet as kc
import heapq

levels = []

#todo: http://stackoverflow.com/questions/5023266/merge-join-two-generators-in-python/5023924#5023924

def multi_merge_join_k(generators):
    #generators = list(generators)
    result = generators.pop()
    while generators:
        result = merge_join_k(result, generators.pop())
    return result


def merge_join_k(left_generator, right_generator):
    stop = False
    while left_generator.get() or right_generator.get():
        try:
            comparison = cmp(right_generator.get_key(), left_generator.get_key())
            if comparison == 0:
                yield left_generator.get_key()
                left_generator.next()
                right_generator.next()
            elif (comparison < 0) or (not left_generator.get() or not right_generator.get()):
                yield right_generator.get_key()
                right_generator.next()   
            else:
                yield left_generator.get_key()
                left_generator.next()    
        except StopIteration:
            if stop:
                raise
            stop = True 

def merge_join_kv(left_generator, right_generator):
    stop = False
    print "mergejoin kv"
    print left_generator
    print right_generator
    print ""
    while left_generator.get() or right_generator.get():
        try:
            comparison = cmp(right_generator.get_key(), left_generator.get_key())
            if comparison == 0:
                yield left_generator.get_key(), left_generator.get_value()
                left_generator.next()
                right_generator.next()
            elif (comparison < 0) or (not left_generator.get() or not right_generator.get()):
                yield right_generator.get_key(), right_generator.get_value()
                right_generator.next()   
            else:
                yield left_generator.get_key(), left_generator.get_value()
                left_generator.next()    
        except StopIteration:
            if stop:
                raise
            stop = True 


             


                        
def multi_merge_join_kv(generators):
    #generators = list(generators)
    result = generators.pop()
    while generators:
        result = merge_join_kv(result, generators.pop())
    return result       

def mergejoin_index(name, options): 
    print "merging " + name
    files = os.listdir(options.input_path)
    test = re.compile(name + ".\d+.\d+.hxidx.kct", re.IGNORECASE)
    to_be_merged = filter(test.search, files)
    db_handlers = []
    cursors = []
    for fi in to_be_merged:
        db = kc.DB()         
        db.open(os.path.join(options.input_path,fi), kc.DB.OREADER)
        print "opened %s containing %i records" % (fi, db.count())
        db_handlers.append(db) 
        cur = db.cursor()
        cur.jump()
        cursors.append(cur)
    
    levels = []
        
    for i in range(0,3):
        levels.append(kc.DB())  
        levels[i].open(os.path.join(options.output_path,name) + str(i) + '.kct#bnum=1000k#psiz=65536#pccap=32m#opts=c', kc.DB.OWRITER | kc.DB.OCREATE) 
 
    
    for full_key in multi_merge_join_k(cursors): 
        if levels[2].get(full_key):            
            print "triple already in the store: %s" % str(triple)
        else:
            levels[2].set(full_key, "") 
            levels[1].increment(full_key[0:(options.key_length*2)], 1) 
            levels[0].increment(full_key[0:(options.key_length)], 1)
            
    for i in levels:
        print "%s has %i records" % (i, i.count())
        i.close()  
    return name + " has merged"        
 






def mergejoin_id2s(options):
    files = os.listdir(options.input_path)
    test = re.compile("id2s.\d+.kct", re.IGNORECASE)
    to_be_merged = filter(test.search, files) 
    
    print [i for i in to_be_merged]
    db_handlers = []
    cursors = []
    for fi in to_be_merged:
        db = kc.DB()         
        db.open(os.path.join(options.input_path,fi), kc.DB.OREADER)
        print "opened %s containing %i records" % (fi, db.count())
        cur = db.cursor()
        cur.jump()
        db_handlers.append(db)
        cursors.append(cur)
    
    #open outfile
    out_db = kc.DB() 
    
    outfilename = os.path.join(options.output_path, "id2s.out.kct")
    out_db.open(outfilename + '#bnum=1000k#psiz=65536#pccap=32m#opts=c', kc.DB.OWRITER | kc.DB.OCREATE)      
    for k,v in multi_merge_join_kv(cursors):
        out_db.set(k,v) 
    out_db.close()   
    return "id2s has merged"
    
                        
def main2(options):
    pool = Pool(processes=2)
    #mergejoin_id2s(options) 
    #mergejoin_index("spo",options)
    results = [] 
    #mergejoin id2s
    res_id2s = pool.apply_async(mergejoin_id2s, [options])
    results.append(res_id2s)
    
    #mergejoin all indexes
    for p in permutations("spo"):
        results.append( pool.apply_async(mergejoin_index, ["".join(p), options]))         
    
    print res_id2s.get()
    for res in results:
        print res.get()
    




def main(): 

    usage = '''converts raw indexes to tygrstore databases.
    input dir should contain files 
    '''
    parser = OptionParser(usage=usage)
    parser.add_option('-l','--key_length', type='long',
                      action='store', dest='key_length', default=16,
                      help='the key length in bytes')
    parser.add_option('-p','--path', type='string',
                   action='store', dest='input_path', default=".",
                   help='path to input files')
    parser.add_option('-o','--output_path', type='string',
                    action='store', dest='output_path', default="./output.hxdb",
                    help='the output path. default: ./output.hxdb')                         

    (options, args) = parser.parse_args()

    t0      = time.time()
    main2(options)
    print 'Took %s seconds'%(str(time.time()-t0))        

    
if __name__ == '__main__':
    main()      