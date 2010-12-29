#import pyximport; pyximport.install()
import pstats, cProfile
import hotshot, hotshot.stats, test.pystone

from stringstore import *
from index_manager import *
from query_engine import *
import time                       
sstore = Stringstore(path="./data_lubm/")
im = IndexManager()
qe = QueryEngine(sstore, im) 

qry = """ SELECT ?publication ?author ?department ?university
WHERE {
        ?publication  <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name> "Publication0" .
        ?publication  <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor> ?author .
        ?author <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor> ?department .
        ?department <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf> ?university .
} LIMIT 100
"""
def testrun(qe, qry):
    res = [i for i in islice(qe.execute(qry),8000)]
    
#cProfile.runctx("testrun()", globals(), locals(), "Profile.prof")

#s = pstats.Stats("Profile.prof")
#s.strip_dirs().sort_stats("time").print_stats()
prof = hotshot.Profile("stones.prof")
prof.runcall(testrun,qe ,qry)
prof.close()
stats = hotshot.stats.load("stones.prof")
stats.strip_dirs()
stats.sort_stats('time', 'calls')
stats.print_stats(20)

start2 = time.clock()
res = [i for i in qe.execute(qry)]
print time.clock() - start2

#print res
print "%s results " % str(len(res)) 

#for i in res:
#    print i
