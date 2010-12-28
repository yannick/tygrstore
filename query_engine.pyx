# cython: profile=True
import roqet
from stringstore import *
import heapq
import pprint  
import time   
import logging
#what happens if a string is not in the store?
# filters, unions, subgraphs?


#bugs: if the same variable is twice in one triple its not working because of triples_containing
class QueryEngine(object):
    
    
    def __init__(self, stringstore, index_manager):
        self.stringstore = stringstore
        self.index_manager = index_manager
        self.logger = logging.getLogger("tygrstore.query_engine")
        
    
        
    def set_index_manager(index_manager):
        self.index_manager = index_manager
        
        
    def execute(self, query):
        #parse the query string with roqet cython bindings
        self.parsed = self.parse(query)
        
        self.triples = zip(self.parsed["pattern"]["triples"], self.encode_triples(self.parsed["pattern"]["triples"]) )
        
        #todo: rename selectivities, its the enriched list of all triple patterns
        # create a list with a dict per entry, keys: original: original triple, encoded: encoded triple, selectivity: the selectivity
        self.selectivities = [ { "original":x[0] , "encoded":x[1], "selectivity" : self.index_manager.selectivity_for_tuple(x[1]) } for x in self.triples] 
        #todo: rewrite, use list comprehensions
        #also add a list of all the vars which a pattern includes, for further ease of access   
        for trip in self.selectivities:
            if "variables" not in trip.keys():
                trip["variables"] = {}
            for tup in trip["original"]:
                #print "doing tuple:"
                #pprint.pprint(tup)
                if tup[0] == 'var':
                    #print "adding %s to variables" % tup[1]
                    trip["variables"][tup[1]] = trip["original"].index(tup)   
                
        #sort by selectivity
        self.selectivities = sorted(self.selectivities, key=lambda x: x["selectivity"])
        self.empty_result_set = {}
         
        #get all unbound variables into empty_result_set    
        for i in self.selectivities:
            for var in i["original"]:
                if var[0] == 'var':
                    self.empty_result_set[var[1]] = None
        
        next_var = [k for k,v in self.empty_result_set.iteritems() if v == None][0] 
        #pprint.pprint(self.selectivities)
        #start = time.time()
        return self.evaluate(self.empty_result_set,next_var) 
        #print "took %s seconds" % str((time.time() - start))
       
    
        
    def evaluate(self, variables_table, var):
        #if var == 'cd':
        #    import pdb; pdb.set_trace()
        #print "eval for var %s and table" % var
        
        #pprint.pprint(variables_table)
         
        if not [i for i in variables_table.values() if i is None]:
            #print "we have produced a result:" 
            #pprint.pprint(self.selectivities)
            #pprint.pprint(variables_table)
            
            for k,v in variables_table.iteritems():
                yield (k, self.stringstore.get(v))
           
            #pprint.pprint(variables_table)
            #for k,v in variables_table.iteritems():
            #    print "key: %s   value: %s" % (k, self.stringstore.get(v))
        else:
            triples_with_var = list(self.triples_containing(var, variables_table))
            #print "all triples with %s" % var
            #pprint.pprint(triples_with_var)
            #for trip in triples_with_var:
            #    pprint.pprint(self.stringstore.convert_tuple(trip))
            for an_id in self.mergejoin_ids(triples_with_var):
                next_var = None
                variables_table[var] = an_id
                #print "var table: for id %s" % an_id
                #pprint.pprint(variables_table)
                try:
                    next_var = [k for k,v in variables_table.iteritems() if v == None].pop()
                except:
                    #print "OMGRESULT"
                    next_var = None
                #print "recursion ++ with id: %s and next var %s" % (an_id, next_var)
                for res in self.evaluate(variables_table, next_var):
                    yield res
                #print "recursion --" 
                variables_table[var] = None     
                                      
    
  
    '''get a tuple of encoded ids which have a certain variable in the original query
     also replace solved variables from the with_solved table with their ids
     ''' 
    def triples_containing(self,var,variables_table):
        for triple in self.selectivities:
            if var in triple["variables"].keys():
                #print "var is in triple"
                encoded_triple = list(triple["encoded"])
                for maybe_solved in triple["variables"].keys():
                    if variables_table[maybe_solved] is not None:
                       encoded_triple[triple["variables"][maybe_solved]] = variables_table[maybe_solved]          
                yield tuple(encoded_triple) 
        raise StopIteration
                
     
    def mergejoin_ids(self,triples_with_var):
        id_generators = [] 
        if len(triples_with_var) == 1:            
            triple =  triples_with_var[0]
            idx = self.index_manager.index_for_tuple(triple)
            selectivity = idx.count(triple)
            return idx.ids_for_triple(triple, num_records=selectivity)
        for triple in triples_with_var:                                                
          #print "list" 
          idx = self.index_manager.index_for_tuple(triple) 
          selectivity = idx.count(triple)
          #pprint.pprint( list( self.index_manager.index_for_tuple(triple).ids_for_triple(triple) ) )
          id_generators.append(idx.ids_for_triple(triple, num_records=selectivity))
        #print "mergejoin"
        #print id_generators  
        return self.multi_merge_join(id_generators)
    
    
    def multi_merge_join(self, generators):
        #generators = list(generators)
        result = generators.pop()
        while generators:
            result = self.merge_join(result, generators.pop())
        return result
            
    
    def merge_join(self, left_generator, right_generator):
        left = left_generator.next()
        right = right_generator.next()
        while left_generator and right_generator:
            comparison = cmp(right, left)
            if comparison == 0:
                yield left
                left = left_generator.next()
                right = right_generator.next()  
            elif comparison > 0:
                left = left_generator.next()
            else:
                right = right_generator.next()
                
    def merge_join_with_jump(self, left_generator, right_generator):
        raise NotImplemented()
        left = left_generator.next()
        right = right_generator.next()
        while left_generator and right_generator:
            comparison = cmp(right, left)
            if comparison == 0:
                yield left
                left = left_generator.next()
                right = right_generator.next()  
            elif comparison > 0:
                left = left_generator.next()
            else:
                right = right_generator.next() 
            
     
    '''replace strings by ids from the stringstore'''       
    def encode_triples(self,triples):
        for triple in triples:                                                     
            #parse the roqet triple to get string or none, replace the string by id and yield a tuple
            yield tuple(self.stringstore.add_generator(self.parse_roqet_triple(triple)))
    
    '''contstruct a tuple of None and Strings
    
    eg.
    (('var', 'x'), ('uri', 'http://mytunes.org/music#artist'),('var', 'y'))
    becomes 
    (None, 'e8de31555c5b36e6933cccb02eaa99126b6cabe7', None)
    '''    
    def parse_roqet_triple(self, triple):
        for tup in triple:
            if tup[0] == 'var':
                yield None            
            elif tup[0] in ('literal','uri'):
                yield tup[1]
            else:
                raise Exception("we only support variables, literals and uris for now, so no %s" % tup[0])
                                         
            
    def parse(self, query):
        try:
            return roqet.sparql(query)
        except:
            self.logger.error("roqet could not parse the query!")
    