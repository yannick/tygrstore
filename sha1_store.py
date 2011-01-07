from rdflib.store import Store
import hashlib

''' this is a dummy store for rdflib to easily add rdf/ntriple files to Tygrstore'''
class Sha1Store(Store):
    
    context_aware = True
    formula_aware = True

    def __init__(self, configuration=None, identifier=None):
        self.outfile = "" 
        self.length = 0

    def bind(self, prefix, namespace): 
        print "bind prefix: %s  to namespace %s" % (prefix, namespace)

    def namespace(self, prefix):
        print "namespace called" 
        return None

    def prefix(self, namespace): 
        print "prefix called"
        return None

    def namespaces(self): 
        print "namespaces called"
        raise StopIteration() 

    def add(self, triple_in, context, quoted=False): 
        #print "triple: %s %s %s\n" % triple_in          
        subj = str(triple_in[0])
        pred = str(triple_in[1])
        obje = str(triple_in[2])
        #triple = (self.stringstore.add(subj), self.stringstore.add(pred), self.stringstore.add(obje)) 
        self.slow_add_to_tygrstore((subj,pred,obje))
        self.length += 1
        if self.length % 100000 == 0: 
            print "adding %s %s %s as %s %s %s" % (subj, pred, obje, hashlib.sha1(subj).hexdigest(),hashlib.sha1(pred).hexdigest(),hashlib.sha1(obje).hexdigest() )

    def slow_add_to_tygrstore(self, triple):
        encoded_triple = tuple(self.stringstore.add_generator(triple))
        self.index_manager.add_to_all_indexes(encoded_triple)
        
    def remove(self, triplepat, context=None): 
        print "remove called"
        pass

    def triples(self, triplein, context=None):    
        print "triples called"
        return self.__emptygen()



    def contexts(self, triple=None):
        print "contexts called"
        return self.__emptygen()

    def __len__(self, context=None): 
        print "len called"
        return self.length

    # internal utility methods below
    

    def __emptygen(self): 
        """return an empty generator"""
        if False:
            yield

