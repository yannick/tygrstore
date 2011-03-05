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
import collections

levels = []

#todo: http://stackoverflow.com/questions/5023266/merge-join-two-generators-in-python/5023924#5023924
class IterableCursor(kc.Cursor, collections.Iterator):
    def __init__(self, *args, **kwargs):
        kc.Cursor.__init__(self, *args, **kwargs)
        collections.Iterator.__init__(self)
        self.jump()        
    def next(self):
        res = self.get(True)
        if res is None:
            raise StopIteration
        else:
            return res
            
def multi_merge_join_k(generators):
    #generators = list(generators)
    result = generators.pop()
    while generators:
        result = merge_join_k(result, generators.pop())
    return result


def merge_join_k(left_generator, right_generator):
    left = left_generator.next()
    right = right_generator.next() 
    stop = False
    while left_generator or right_generator:
        comparison = cmp(right, left)
        if comparison == 0:
            try:
                left = left_generator.next()
            #if left is empty we need to empty right too
            except StopIteration:
                for i in right_generator:
                    yield i
                raise StopIteration   
            try:
                right = right_generator.next() 
            #if left is empty we need to empty right too 
            except StopIteration:
                for i in left_generator:
                    yield i
                raise StopIteration 
            yield left
        elif comparison < 0:
            yield right
            try:
                right = right_generator.next()
            except StopIteration:
                for i in left_generator:
                    yield i
                raise StopIteration
        else:
            yield left
            try:
                left = left_generator.next()
            except StopIteration:
                for i in right_generator:
                    yield i
                raise StopIteration 

def merge_join_kv(left_generator, right_generator):
    left = left_generator.next()
    right = right_generator.next() 
    stop = False
    while left_generator or right_generator:
        comparison = cmp(right[0], left[0])
        if comparison == 0:
            try:
                left = left_generator.next()
            #if left is empty we need to empty right too
            except StopIteration:
                for i in right_generator:
                    yield i
                raise StopIteration   
            try:
                right = right_generator.next() 
            #if left is empty we need to empty right too 
            except StopIteration:
                for i in left_generator:
                    yield i
                raise StopIteration 
            yield left
        elif comparison < 0:
            yield right
            try:
                right = right_generator.next()
            except StopIteration:
                for i in left_generator:
                    yield i
                raise StopIteration
        else:
            yield left
            try:
                left = left_generator.next()
            except StopIteration:
                for i in right_generator:
                    yield i
                raise StopIteration     


             


                        
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
        cur = IterableCursor(db)
        cursors.append(cur)
    
    levels = []
        
    for i in range(0,3):
        levels.append(kc.DB())  
        levels[i].open(os.path.join(options.output_path,name) + str(i) + '.kct#bnum=1000k#psiz=65536#pccap=32m#opts=c', kc.DB.OWRITER | kc.DB.OCREATE) 
 
    
    for full_key in multi_merge_join_k(cursors):
        print "fullkey has length: %i" %  len(full_key) 
        raise "fuck" 
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
    test = re.compile("id2s.\d+.kch", re.IGNORECASE)
    to_be_merged = filter(test.search, files) 
    
    print [i for i in to_be_merged]
    db_handlers = []
    cursors = []
    for fi in to_be_merged:
        db = kc.DB()         
        db.open(os.path.join(options.input_path,fi), kc.DB.OREADER)
        print "opened %s containing %i records" % (fi, db.count())
        cur = IterableCursor(db)
        cursors.append(cur)
    
    #open outfile
    out_db = kc.DB() 
    
    outfilename = os.path.join(options.output_path, "id2s.kch")
    out_db.open(outfilename + '#bnum=1000k#psiz=65536#pccap=32m#opts=c', kc.DB.OWRITER | kc.DB.OCREATE) 
     
    for k,v in multi_merge_join_kv(cursors):
        out_db.set(k,v) 
    out_db.close()   
    return "id2s has merged"
    
                        
def main2(options):
    pool = Pool(processes=options.cpus)
 
    #mergejoin_index("spo",options)
    results = [] 
    #mergejoin id2s
    res_id2s = pool.apply_async(mergejoin_id2s, [options])
    results.append(res_id2s)
    
    #mergejoin all indexes
    for p in permutations("spo"):
        results.append( pool.apply_async(mergejoin_index, ["".join(p), options]))         
    
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
    
    parser.add_option('-c','--cpus', type='long',
                    action='store', dest='cpus', default="4",
                    help='the number of cpus, default: 4') 
    (options, args) = parser.parse_args()

    t0      = time.time()
    main2(options)
    print 'Took %s seconds'%(str(time.time()-t0))        

    
if __name__ == '__main__':
    main()      