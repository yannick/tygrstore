# cython: profile=True
from stringstore import *
import stringstore
import heapq
import pprint  
import time   
import logging
import cysparql as sparql
import binascii 
from helpers import *
class BGP(object):
      def __init__(self, context=None, optional=False):
          self.optional = optional
          self.context = context
          self.triples = []
          
      
class ResultSet(object):
    def __init__(self,variables, triples):
        self.triples = triples
        self.variables = list(variables)
        self.unsolved_variables = list(variables)
        self.solutions = dict((var,None) for var in variables)     
        
    def triples_with_var(self,var):
        return [triple for triple in self.triples if (triple.unsolved_variables.count(var) > 0 )]
                             
    '''set the variable as resolved'''
    def resolve(self, var, solution):
        #import pdb; pdb.set_trace()
        for triple in self.triples:
            triple.resolve(var, solution)
        #try:
        self.unsolved_variables.remove(var)
        #except ValueError:
        #    pass #just setting a new value
        self.solutions[var] = solution   
    
    def unresolve(self, var):
        for triple in self.triples:
            triple.unresolve(var)
        self.unsolved_variables.insert(0,var)        
        self.solutions[var] = None
            
    def __str__(self):
        return "ResultSet with unsolved: %s and %s triples" % (str(self.unsolved_variables), len(self.triples))
           
class Triple(object):
    def __init__(self,cysparql_triple):
        #import pdb; pdb.set_trace()
        self.ids = list(cysparql_triple.as_id_tuple()) #self.stringstore.get_ids_from_tuple( cysparql_triple.n3(withvars=False) )
        self.variables = list(cysparql_triple.variables)
        self.variables_tuple = cysparql_triple.as_var_tuple()
        print str(self.variables)
        self.n3 = cysparql_triple.n3(withvars=False)
        self.unsolved_variables = list(self.variables)
        self.selectivity = cysparql_triple.selectivity
        
    def __str__(self):
        return "triple: " + str(self.n3) + " variables: " + str(self.variables)
    
    def resolve(self,var,solution):
        if self.variables.count(var) > 0:         
            self.ids[self.variables_tuple.index(var)] = solution
            self.unsolved_variables.remove(var)
    
    def unresolve(self,var):
        if self.variables.count(var) > 0:
            self.unsolved_variables.insert(0,var)
            self.ids[self.variables_tuple.index(var)] = None   
    
    def ids_as_tuple(self):
        return tuple(self.ids) 
                        
class QueryEngine(object):
    
    
    def __init__(self, stringstore, index_manager, config_file):
        self.stringstore = stringstore
        self.index_manager = index_manager
        self.logger = logging.getLogger("tygrstore.query_engine")
        self.config = config_file
    
    def execute2(self, query):
        #parse the query
        self.sparql_query = sparql.Query(query)
         

        
        triples = []    
        
        current_gp = self.sparql_query.graph_pattern
        #encode and set selectivity  
        self.logger.debug("encoding triples and get selectivities")
        for triple in current_gp:
            #encode triples 
            #self.logger.debug("using triple %s %s %s" % triple.n3(withvars=True))
            ids = self.stringstore.get_ids_from_tuple( triple.n3(withvars=False) )
            #self.logger.debug("spo triple %s,%s,%s" % (triple.s.value, triple.p.value, triple.o.value)) 
            triple.encode(ids[0],ids[1],ids[2], numeric=eval(self.config.get("general", "numeric_ids")))
            #get selectivities 
            sel = self.index_manager.selectivity_for_tuple(ids)
            self.logger.debug("got selectivity of %s" % sel)
            triple.selectivity = sel    
            #create a new Triple object and add it to all triples
            triples.append(Triple(triple))   
            
        #put all variables into a dict
        #old self.empty_result_set = dict( (var.name, None) for var in self.sparql_query.vars)
        #self.logger.debug("sorting by selectivity")    
        triples = sorted(triples, key=lambda a_triple: a_triple.selectivity) 
        #import pdb; pdb.set_trace()    
        empty_result_set = ResultSet([var.name for var in self.sparql_query.vars], triples)   
            
                
        
        firstvar = empty_result_set.triples[0].variables[0]
        #self.logger.debug("choosing %s as first variable to solve" % firstvar)
        #empty_result_set = dict([(var.name,None) for var in self.sparql_query.vars])
        #self.logger.debug("calling evaluate2 with " + str(empty_result_set) + "firstvar: " + firstvar + " triples: " + str(triples))   
        for res in self.evaluate2(empty_result_set, firstvar):             
            yield self.id2s_hash(res)
                
    '''the recursively called evaluate function'''    
    def evaluate2(self, result_set, var):
        #self.logger.debug("evaluate2 for var " + str(var) + " and triples: " + str(result_set.triples_with_var(var)))  
        #self.logger.debug("----solutions: " + str(result_set.solutions)   + "\n-----") 
        #if var == "x":
        #    import pdb; pdb.set_trace()
        #TODO: OPTIMIZE, a lot of time is lost here   
        #if there are no unsolved variables then return a result set
        #this could be moved down the line to save a recursion step
        #if len(result_set.unsolved_variables) == 0:
        #    self.logger.debug("FOUND RESULTS!")
        #    yield result_set.solutions
        #else:
        #we need only the triples which contain the unbound variable we search  
        # triple.n3(withvars=False)
        #triples_with_var = [triple.as_id_tuple() for triple in triples if (var in triple.variables)]
        triples_with_var = result_set.triples_with_var(var) 
      
        #we then join all the resulting id's                                                        
        for an_id in self.mergejoin_ids(triples_with_var, var):                                          
            #import pdb; pdb.set_trace()                                                                         
            #set the var as solved                                                                  
            result_set.resolve(var, an_id) 
            next_var = None
            if len(result_set.unsolved_variables) > 0:                                                        
                next_var =  result_set.unsolved_variables[0]                                                  
                #print "recursion ++ with new var %s" % next_var
                #self.logger.debug("recursion ++ with new var %s" % next_var)
                for res in self.evaluate2(result_set, next_var):                                    
                    yield res                                                                           
                #self.logger.debug("recursion --")                                                                  
                #unset the just solved var                                                              
                result_set.unresolve(var)
            else:
                 #self.logger.debug("FOUND RESULTS!")
                 #self.logger.debug("----solutions: " + str(result_set.solutions)   + "\n-----")                  
                 yield result_set.solutions
                 #import pdb; pdb.set_trace() 
                 result_set.unresolve(var)
                 #
                 # self.logger.debug("UNRESOLVE %s" % var)
 
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
            id_generators.append(self.index_manager.ids_for_ttriple(triple, var))
        return self.multi_merge_join(id_generators)
    
    
    def multi_merge_join(self, generators):
        #generators = list(generators)
        
        result = generators.pop()
        
        while len(generators) > 0:
            result = self.merge_join(result, generators.pop())
        return result
            
    
    def merge_join(self, left_generator, right_generator):
        #cdef char* left
        #cdef char* righ  
        
        left = left_generator.next()
        right = right_generator.next()
        #print "left:  %s" % binascii.hexlify(left)
        #print "right: %s" % binascii.hexlify(right)
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
                left = left_generator.send(right)
            else:
                right = right_generator.send(left) 
            
    
    #old 
    '''replace strings by ids from the stringstore'''       
    def encode_triples(self,triples):
        for triple in triples:                                                     
            #parse the roqet triple to get string or none, replace the string by id and yield a tuple
            yield tuple(self.stringstore.add_generator(self.parse_roqet_triple(triple)))
     
    def encode_query(self):
        for triple in self.sparql_query:
            triple.encoded = self.stringstore.get_ids_from_tuple(triple.tuple_of_strings)
    
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
    
