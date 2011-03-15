import binascii as ba
import functools
import logging 
from operator import itemgetter, attrgetter

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
        self.logger = logging.getLogger("tygrstore.result_set") 
        
        #OMG please ignore this
        # for triple in self.triples:
        #     for varname in triple.variables_tuple:
        #         for varobj in self.unsolved_variables:
        #             if varobj.selectivity > triple.selectivity or varobj.selectivity < 0:
        #                 varobj.selectivity = triple.selectivity  
                        
        #sort the unresolved variables
        #sorted( self.unsolved_variables, key=attrgetter("selectivity"), reverse=True)
        
    def triples_with_var(self,var):
        return [triple for triple in self.triples if (triple.unsolved_variables.count(var) > 0 )] 
    
    def unsolved_triples(self):
        for t in self.triples:
            if len(t.unsolved_variables) > 0:
                yield t
            
    def get_most_selective_var(self): 
        least_selective_triple = min(self.unsolved_triples(), key = lambda x: x.selectivity )
        #self.logger.debug("found least selective triple " + str(least_selective_triple.n3) + " with variables: " + str(least_selective_triple.variables_tuple)  ) 
        #import pdb; pdb.set_trace() 
        if len(least_selective_triple.unsolved_variables) == 1:
            #self.logger.debug("one var:: " + least_selective_triple.variables_tuple[0])
            return least_selective_triple.unsolved_variables[0]
        else:
           #get the var with the most triples!
           var_with_min_triple = min( least_selective_triple.variables_tuple, key = lambda y:  len([1 for x in self.triples if y in x.variables_tuple]) )
           self.logger.debug("multiple vars! choose: " + var_with_min_triple)
           return var_with_min_triple  
                             
    '''set the variable as resolved'''
    def resolve(self, var, solution):
        #import pdb; pdb.set_trace()
        for triple in self.triples:
            if var in triple.unsolved_variables:            
                triple.resolve(var, solution)
                #explore this path 
                # import pdb; pdb.set_trace()
                #                 for var2 in triple.unsolved_variables: 
                #                     #move the var to the end of the list to be solved next!
                #                     self.unsolved_variables.append( self.unsolved_variables.pop(self.unsolved_variables.index(var2)))                               
        #try:
        self.unsolved_variables.remove(var)
        #except ValueError:
        #    pass #just setting a new value
        self.solutions[var] = solution   
    
    def unresolve(self, var):
        for triple in self.triples:
            triple.unresolve(var)
        self.unsolved_variables.append(var)        
        self.solutions[var] = None
            
    def __str__(self):
        return "ResultSet with unsolved: %s and %s triples" % (str(self.unsolved_variables), len(self.triples))
           
class Triple(object):
    def __init__(self,cysparql_triple):
        self.ids = list(cysparql_triple.as_id_tuple()) 
        self.variables = list(cysparql_triple.variables)
        self.variables_tuple = cysparql_triple.as_var_tuple()
        self.n3 = cysparql_triple.n3(withvars=False)
        self.unsolved_variables = list(self.variables)
        self.selectivity = cysparql_triple.selectivity
        self.updated = False
        
    def __str__(self):
        return "triple: " + str(self.n3) + " variables: " + str(self.variables)
    
    def update_selectivity(self, new_sel):
        self.updated = False
        self.selectivity = new_sel
        
    def resolve(self,var,solution):
        if self.variables.count(var) > 0:         
            self.ids[self.variables_tuple.index(var)] = solution            
            self.unsolved_variables.remove(var)
            self.updated = True
    
    def unresolve(self,var):
        if self.variables.count(var) > 0:
            self.unsolved_variables.append(var)
            self.ids[self.variables_tuple.index(var)] = None 
            self.updated = True  
    
    def ids_as_tuple(self):
        return tuple(self.ids)
        
def pp_tuple(triple):
    return tuple(i for i in t2hex(triple))

def pp_id2s(triple,stringstore):
    return tuple(stringstore.id2s(i) for i in triple)
        
def t2hex(triple):
    for key in triple:
        if key is None:
            yield "None"
        else:
            yield ba.hexlify(key)


class memoized(object):
   """Decorator that caches a function's return value each time it is called.
   If called later with the same arguments, the cached value is returned, and
   not re-evaluated.
   """
   def __init__(self, func):
      self.func = func
      self.cache = {}
   def __call__(self, *args):
      try:
         return self.cache[args]
      except KeyError:
         value = self.func(*args)
         self.cache[args] = value
         return value
      except TypeError:
         # uncachable -- for instance, passing a list as an argument.
         # Better to not cache than to blow up entirely.
         return self.func(*args)
   def __repr__(self):
      """Return the function's docstring."""
      return self.func.__doc__
   def __get__(self, obj, objtype):
      """Support instance methods."""
      return functools.partial(self.__call__, obj)   
      
      
class TStats(object):
    def __new__(cls, *p, **k):
        if not '_the_instance' in cls.__dict__:
            cls._the_instance = object.__new__(cls)
        return cls._the_instance  
         
    def __init__(self):
        self.stats = {} 
        
    def increment(self,key, val):
        try:
            self.stats[key] += val
        except KeyError:
           self.stats[key] = val
                
      
             