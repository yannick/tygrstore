__author__ = 'Yannick Koechlin'
__email__ = 'yannick@koechlin.name'
# # cython: profile=True
from stringstore import *
import stringstore
import heapq
import pprint  
import time   
import logging
import cysparql as sparql
import binascii 
from helpers import *
import stopwatch
                        
class QueryEngine(object):
    
    
    def __init__(self, stringstore, index_manager, config_file):
        self.stringstore = stringstore
        self.index_manager = index_manager
        self.logger = logging.getLogger("tygrstore.query_engine")
        self.config = config_file 
        self.jump_btree = self.config.getboolean("index", "jump_btree")
        
        self.stats = TStats()
        
    def reload_sparql(self):
        reload(sparql)
    
    def execute(self, query):
        #parse the query
        self.sparql_query = sparql.Query(query)
               
        triples = []    
        
        setup_time = stopwatch.Timer()
        #TODO: set together the different graph patterns and execute their code in paralell
        current_gp = self.sparql_query.graph_pattern                                       
        
        #encode and set selectivity  
        self.logger.debug("encoding triples and get selectivities")
        for triple in current_gp:
            #encode triples 
            ids = self.stringstore.get_ids_from_tuple( triple.n3(withvars=False) ) 
            triple.encode(ids[0],ids[1],ids[2], numeric=eval(self.config.get("general", "numeric_ids"))) 
            
            #get selectivity 
            sel = self.index_manager.selectivity_for_tuple(ids) 
            #sel = 2           
            triple.selectivity = sel
            self.logger.debug("got selectivity of %s for tuple %s" % (sel,triple.n3(withvars=True)))
                 
            #create a new Triple object and add it to all triples
            triples.append(Triple(triple))   
            

        #sort triples by selectivity
        triples = sorted(triples, key=lambda a_triple: a_triple.selectivity)  
        
        #generate an empty result set
        empty_result_set = ResultSet([var.name for var in self.sparql_query.vars], triples)   
        #empty_result_set = ResultSet( triples)     
                
        #import pdb; pdb.set_trace() 
        #get the first variable we want to solve
         
        firstvar = empty_result_set.get_most_selective_var() 
        
        #for debugging 
        self.recursionsteps = 0
        self.checkvar = ""
        self.logger.debug("got the following variables: %s and using %s as the first" % (str(empty_result_set.unsolved_variables),firstvar))  
        self.t0 = time.time()
        #generator which yields the actual results as hash
        self.stats.stats["qe_setup_time"] = setup_time.elapsed         
        for res in self.evaluate(empty_result_set, firstvar):                         
            yield (self.id2s_hash(res), self.recursionsteps)              
        
                
    '''the recursively called evaluate function'''    
    def evaluate(self, result_set, var):
        
        # if (var != self.checkvar): 
        #     self.logger.debug( str(time.time() - self.t0) + "solving variable: " + var)
        #     self.checkvar = var     
        self.stats.increment("calls_to_evaluate_by_var." + var, 1)
         
        #TODO: check if only 1 triple in BGP
        
        #we need only the triples which contain the unbound variable we search  
        triples_with_var = result_set.triples_with_var(var) 
        #self.logger.debug( str(time.time() - self.t0) + "got triples_with_var for " + var)
        #we then join all the resulting id's                                                        
        for an_id in self.mergejoin_ids(triples_with_var, var):                                                                                                             
            #set the var as solved  
            #self.logger.debug( str(time.time() - self.t0) + "next id for: " + var)                                                                
            result_set.resolve(var, an_id) 
            next_var = None                                                                    
             
            
            for ttriple in result_set.triples:
                if ttriple.updated:
                    ttriple.update_selectivity(
                        self.index_manager.selectivity_for_tuple(ttriple.ids_as_tuple()) 
                        )  
            #self.logger.debug( str(time.time() - self.t0) + "updated selectivities")        
            #if we have unsolved variables go a recursion step deeper, otherwise yield a result
            if len(result_set.unsolved_variables) > 0:                                                        
                next_var =  result_set.get_most_selective_var()                                                  
                
                #recursive call!
                for res in self.evaluate(result_set, next_var):                                    
                    yield res                                                                    
                #unset the just solved var                                                              
                result_set.unresolve(var) 
                #self.logger.debug("unsert: " + str(var))
                #if var == 'student':
                #    import pdb; pdb.set_trace()
            else:
                #we found a result                
                yield result_set.solutions
                result_set.unresolve(var)
        
 
    def id2s_hash(self, solution):
        return dict( (k, self.stringstore.id2s(v)) for k,v in solution.iteritems())
                                                            
                                                                                            
 
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
                
     
    def mergejoin_ids(self,triples_with_var, var):        
        id_generators = []  
        if len(triples_with_var) == 1:            
            return self.index_manager.ids_for_ttriple(triples_with_var[0], var)
        for triple in triples_with_var:                                                
            id_generators.insert(0,self.index_manager.ids_for_ttriple(triple, var)) 
        #self.logger.debug( "joining %s generators" % len(id_generators))
        return self.multi_merge_join(id_generators)
    
    
    def multi_merge_join(self, generators):
        #generators = list(generators)        
        result = generators.pop()
        if self.jump_btree:
            while len(generators) > 0:
                result = self.merge_join_with_jump(result, generators.pop())
        else:
            while len(generators) > 0:
                result = self.merge_join(result, generators.pop()) 
        return result
            
    
    #todo: remove jumping
    def merge_join(self, left_generator, right_generator):
        #cdef char* left
        #cdef char* righ  
        
        left = left_generator.next()
        right = right_generator.next()
        while left_generator and right_generator:
            #self.logger.debug("comparing: %s to %s" % (self.stringstore.id2s(left), self.stringstore.id2s(right)))
            comparison = cmp(right, left)
            if comparison == 0:
                #print "MATCH"
                yield left
                left = left_generator.next()
                right = right_generator.next()  
            elif comparison > 0:
                #print "sending right to left"
                left = left_generator.send(right)
            else:
                #print "sending left to right"
                right = right_generator.send(left)
                
    def merge_join_with_jump(self, left_generator, right_generator):
        left = left_generator.next()
        right = right_generator.next()                               
        while left_generator and right_generator:
            comparison = cmp(right, left)
            #self.logger.debug(" left: %s     right: %s " %  ( binascii.hexlify(left), binascii.hexlify(right) ) )
            if comparison == 0:                
                yield left
                left = left_generator.next()
                right = right_generator.next() 
            elif comparison > 0:
                left = left_generator.send(right)
            else:
                right = right_generator.send(left) 
            
    

    
