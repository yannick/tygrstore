import rdflib
from rdflib import *  
from rdflib import plugin
from sha1_store import *                   
from stringstore import *
from index_manager import * 
from query_engine import *   

stringstore = Stringstore(mode="sha1", store="tc", path="./data_lubm/")
index_manager = IndexManager()
qu = QueryEngine()
qe.stringstore = stringstore
qe.index_manager = index_manager

plugin.register( 'Sha1Store', rdflib.store.Store,'sha1_store', 'Sha1Store')
g = Graph(store="Sha1Store")
#stringstore = Stringstore(mode="sha1", store="tc", path="./data_lubm/")
#index_manager = IndexManager()
g.store.index_manager = index_manager
g.store.stringstore = stringstore
g.parse("data_lubm/lubm-sorted.nt", format="nt")

#zzZZZZzzzz


qry = """ SELECT ?publication ?author ?department ?university
WHERE {
        ?publication  <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name> "Publication0" .
        ?publication  <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor> ?author .
        ?author <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor> ?department .
        ?department <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf> ?university .
} LIMIT 100
"""

#4 unbound vars and limit 100 -> list of size 400  (-> emulating the LIMIT for now)
res = [i for i in islice(qe.execute(qry),400)]





