from crasqal cimport *
from cython cimport *
from cpython cimport *
from libc.stdio cimport *
from libc.stdlib cimport *
from itertools import *
import os
import sys

__author__ = 'Cosmin Basca'
__email__ = 'basca@ifi.uzh.ch; cosmin.basca@gmail.com'



cpdef get_q():
    return Query('''
SELECT ?title_other ?title ?author
WHERE {
        ?paper <http://www.aktors.org/ontology/portal#has-title> ?title .
        ?paper <http://www.aktors.org/ontology/portal#has-author> ?author .
        ?paper <http://www.aktors.org/ontology/portal#article-of-journal> ?journal .
        ?paper <http://www.aktors.org/ontology/portal#has-date> <http://www.aktors.org/ontology/date#2009> .
        ?paper_other <http://www.aktors.org/ontology/portal#article-of-journal> ?journal .
        ?paper_other <http://www.aktors.org/ontology/portal#has-title> ?title_other .
} LIMIT 100
    ''')

cpdef get_q2():
    return Query('''
SELECT ?seed ?modified ?common_taxon
WHERE {
        ?cluster <http://www.w3.org/2000/01/rdf-schema#label> ?label .
        ?cluster <http://purl.uniprot.org/core/member> ?member .
        ?member <http://purl.uniprot.org/core/seedFor> ?seed .
        ?seed <http://purl.uniprot.org/core/modified> ?modified .
        ?cluster <http://purl.uniprot.org/core/commonTaxon> ?common_taxon .
        ?cluster <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.uniprot.org/core/Cluster> .
} LIMIT 100
    ''')


def benchmark_query(debug=False):
    q = get_q()
    if debug: q.debug()

def benchmark(nr=1000):
    from timeit import Timer
    t = Timer('benchmark_query(debug=False)','from crasqal import benchmark_query')
    total_secs = t.timeit(number=nr)
    print 'Query parsing took %s ms, with a total %s seconds for %s runs.'%(str(1000 * total_secs/nr), str(total_secs), str(nr))


#-----------------------------------------------------------------------------------------------------------------------
# Enums - CONSTANTS
#-----------------------------------------------------------------------------------------------------------------------
ctypedef public enum Selectivity:
    SELECTIVITY_UNDEFINED = -2
    SELECTIVITY_ALL_TRIPLES = -1
    SELECTIVITY_NO_TRIPLES = 0

ctypedef public enum GraphPatternOperator:
    OPERATOR_UNKNOWN = RASQAL_GRAPH_PATTERN_OPERATOR_UNKNOWN
    OPERATOR_BASIC = RASQAL_GRAPH_PATTERN_OPERATOR_BASIC
    OPERATOR_OPTIONAL = RASQAL_GRAPH_PATTERN_OPERATOR_OPTIONAL
    OPERATOR_UNION = RASQAL_GRAPH_PATTERN_OPERATOR_UNION
    OPERATOR_GROUP = RASQAL_GRAPH_PATTERN_OPERATOR_GROUP
    OPERATOR_GRAPH = RASQAL_GRAPH_PATTERN_OPERATOR_GRAPH
    OPERATOR_FILTER = RASQAL_GRAPH_PATTERN_OPERATOR_FILTER
    OPERATOR_LET = RASQAL_GRAPH_PATTERN_OPERATOR_LET
    OPERATOR_SELECT = RASQAL_GRAPH_PATTERN_OPERATOR_SELECT
    OPERATOR_SERVICE = RASQAL_GRAPH_PATTERN_OPERATOR_SERVICE
    OPERATOR_MINUS = RASQAL_GRAPH_PATTERN_OPERATOR_MINUS
    OPERATOR_LAST = RASQAL_GRAPH_PATTERN_OPERATOR_LAST

#-----------------------------------------------------------------------------------------------------------------------
# Iterators (directly on rasqal sequences)
#-----------------------------------------------------------------------------------------------------------------------
cdef inline uri_to_str(raptor_uri* u):
    return raptor_uri_as_string(u) if u != NULL else None

cdef class SequenceItemType:
    pass

cdef class SequenceIterator:
    cdef rasqal_query* rq
    cdef void* data
    cdef int __idx__

    def __cinit__(self, rq, data):
        self.rq = <rasqal_query*>rq
        self.__idx__ = 0
        self.data = NULL if data is None else <void*>data

    def __iter__(self):
        self.__idx__ = 0
        return self

    cdef inline raptor_sequence* __seq__(self):
        return NULL

    def __item__(self, seq_item):
        return SequenceItemType()

    def __next__(self):
        cdef raptor_sequence* seq =  self.__seq__()
        cdef int sz = 0
        if seq != NULL:
            sz = raptor_sequence_size(seq)
            if self.__idx__ == sz:
                raise StopIteration
            else:
                item = self.__item__(<object>raptor_sequence_get_at(seq, self.__idx__))
                self.__idx__ += 1
                return item
        else:
            raise StopIteration

cdef class AllVarsIterator(SequenceIterator):
    cdef inline raptor_sequence* __seq__(self):
        return rasqal_query_get_all_variable_sequence(self.rq)

    def __item__(self, seq_item):
        return Variable(seq_item)


cdef class BoundVarsIterator(SequenceIterator):
    cdef inline raptor_sequence* __seq__(self):
        return rasqal_query_get_bound_variable_sequence(self.rq)

    def __item__(self, seq_item):
        return Variable(seq_item)


cdef class BindingsVarsIterator(SequenceIterator):
    cdef inline raptor_sequence* __seq__(self):
        return rasqal_query_get_bindings_variables_sequence(self.rq)

    def __item__(self, seq_item):
        return Variable(seq_item)


cdef class QueryTripleIterator(SequenceIterator):
    cdef inline raptor_sequence* __seq__(self):
        return rasqal_query_get_triple_sequence(self.rq)

    def __item__(self, seq_item):
        return Triple(seq_item)


cdef class GraphPatternIterator(SequenceIterator):
    cdef inline raptor_sequence* __seq__(self):
        return rasqal_graph_pattern_get_sub_graph_pattern_sequence(<rasqal_graph_pattern*>self.data)

    def __item__(self, seq_item):
        return GraphPattern(<object>self.rq, seq_item)

#-----------------------------------------------------------------------------------------------------------------------
# RASQAL WORLD
#-----------------------------------------------------------------------------------------------------------------------
cdef class RasqalWorld:
    cdef rasqal_world* rw

    def __cinit__(self):
        self.rw  = rasqal_new_world()

    def __dealloc__(self):
        rasqal_free_world(self.rw)

    def __str__(self):
        return '"RasqalWorld wrapper"'

#-----------------------------------------------------------------------------------------------------------------------
# RDF TYPES
#-----------------------------------------------------------------------------------------------------------------------
cdef class IdContainer:
    cdef long __numid__
    cdef bytes __hashid__

    def __cinit__(self):
        self.__numid__ = 0
        self.__hashid__ = None

    property numeric_id:
        def __get__(self):
            return self.__numid__

        def __set__(self, v):
            self.__numid__ = <long>v

    property hash_id:
        def __get__(self):
            return self.__hashid__

        def __set__(self, v):
            self.__hashid__ = v


cdef class Term(IdContainer):
    def __val__(self):
        return None

    property value:
        def __get__(self):
            return self.__val__()

    def n3(self):
        return ''

cdef class Uri(Term):
    cdef bytes __uri__

    def __cinit__(self, u):
        self.__uri__ = u

    def __val__(self):
        return self.__uri__

    def n3(self):
        return '<%s>'%self.__uri__

    property uri:
        def __get__(self):
            return self.__uri__


cdef class BNode(Uri):
    def n3(self):
        return '_:%s'%<str>self.uri

    property id:
        def __get__(self):
            return <str>self.uri


cdef class Literal(Term):
    cdef bytes val
    cdef bytes lang
    cdef Uri dtype
    
    def __cinit__(self, val, lang, dtype):
        self.val = val
        if dtype:
            self.lang = None
            self.dtype = dtype if type(dtype) is Uri else Uri(dtype)
        else:
            self.dtype = None
            self.lang = lang if lang else None

    property lang:
        def __get__(self):
            return self.lang if self.lang is not None else None

    property datatype:
        def __get__(self):
            return self.dtype

    def __val__(self):
        return self.val

    def n3(self):
        repr = '%s'%self.val
        if self.lang is not None:
            repr += '@%s'%self.lang
        elif self.dtype:
            repr += '^^%s'%self.dtype.n3()
        return repr

#-----------------------------------------------------------------------------------------------------------------------
# QUERY LITERAL
#-----------------------------------------------------------------------------------------------------------------------
cdef class QueryLiteral:
    cdef rasqal_literal* l
    
    property language:
        def __get__(self):
            return self.l.language if self.l.language != NULL else None

    property datatype:
        def __get__(self):
            return uri_to_str(self.l.datatype) if self.l.datatype != NULL else None

    property type:
        def __get__(self):
            return self.l.type

    property type_label:
        def __get__(self):
            return rasqal_literal_type_label(self.l.type)

    cpdef is_rdf_literal(self):
        return True if rasqal_literal_is_rdf_literal(self.l) > 0 else False

    cpdef as_var(self):
        cdef rasqal_variable* var = rasqal_literal_as_variable(self.l)
        return Variable(<object>var) if var != NULL else None
    
    cpdef var_as_string(self):
        if self.l.type == RASQAL_LITERAL_VARIABLE: return self.l.value.variable.name
        return None
    
    cpdef as_str(self):
        if self.l.type == RASQAL_LITERAL_URI or self.l.type == RASQAL_LITERAL_BLANK:
            return rasqal_literal_as_string(self.l)
        elif self.l.type == RASQAL_LITERAL_VARIABLE:
            return self.l.value.variable.name if self.l.value.variable.name != NULL else ''
        return ''

    cpdef as_node(self):
        cdef rasqal_literal* node = rasqal_literal_as_node(self.l)
        return new_queryliteral(node) if node != NULL else None

    def __str__(self):
        return self.as_str()

    property value:
        def __get__(self):
            if self.l.type == RASQAL_LITERAL_URI:
                #return rasqal_literal_as_string(self.l)
                return Uri(rasqal_literal_as_string(self.l))
            elif self.l.type == RASQAL_LITERAL_BLANK:
                return BNode(rasqal_literal_as_string(self.l))
            elif self.l.type == RASQAL_LITERAL_STRING:
                return Literal(<object>self.l.string, None, None)
            #elif self.l.type == RASQAL_LITERAL_INTEGER:
            #    return self.l.value.integer
            #elif self.l.type == RASQAL_LITERAL_FLOAT or self.l.type == RASQAL_LITERAL_DOUBLE:
            #    return self.l.value.floating
            elif self.l.type == RASQAL_LITERAL_VARIABLE:
                return Variable(<object>self.l.value.variable)
            return None
            
    cpdef debug(self):
        rasqal_literal_print(<rasqal_literal*>self.l, stdout)

all_literals = {}
cdef QueryLiteral new_queryliteral(rasqal_literal* l): 
    cdef long addr = <long>l     
    if all_literals.has_key(addr): return all_literals[addr]   
    cdef QueryLiteral ql = QueryLiteral.__new__(QueryLiteral)        
    ql.l = l 
    all_literals[addr] = ql
    return ql 

#-----------------------------------------------------------------------------------------------------------------------
# VARIABLE
#-----------------------------------------------------------------------------------------------------------------------
cdef class Variable(IdContainer):
    cdef rasqal_variable* var
    cdef bint __resolved__ 
    cdef public long selectivity

    def __cinit__(self, var):
        self.var = <rasqal_variable*>var
        self.__resolved__ = False
        self.selectivity = -2
    
    cdef rasqal_variable* get_var(self):
        return <rasqal_variable*> self.var

        
    property name:
        def __get__(self):
            return self.var.name if self.var.name != NULL else None

    property offset:
        def __get__(self):
            return self.var.offset

    property value:
        def __get__(self):
            return new_queryliteral(self.var.value) if self.var.value != NULL else None

    cpdef debug(self):
        rasqal_variable_print(<rasqal_variable*>self.var, stdout)

    def __str__(self):
        return '(VAR %s, resolved=%s, selectivity=%s, id=%s)'%(self.name, bool(self.__resolved__), str(self.selectivity), str(self.numeric_id))

    property resolved:
        def __get__(self):
            return self.__resolved__

        def __set__(self, v):
            self.__resolved__ = v

    def n3(self):
        return '?%s'%<str>self.var.name # not really valid N3

    cpdef is_not_selective(self):
        return True if self.selectivity == SELECTIVITY_NO_TRIPLES else False

    cpdef is_all_selective(self):
        return True if self.selectivity == SELECTIVITY_ALL_TRIPLES else False

    cpdef is_undefined_selective(self):
        return True if self.selectivity == SELECTIVITY_UNDEFINED else False

    property id:
        def __get__(self):
            return self.numeric_id

        def __set__(self,v):
            self.numeric_id = <long>v


#-----------------------------------------------------------------------------------------------------------------------
# TRIPLE
#-----------------------------------------------------------------------------------------------------------------------
cdef class Triple:
    cdef rasqal_triple*         t
    cdef int                    __idx__
    cdef public QueryLiteral    s_qliteral
    cdef public IdContainer     s
    cdef public QueryLiteral    p_qliteral
    cdef public IdContainer     p
    cdef public QueryLiteral    o_qliteral
    cdef public IdContainer     o
    cdef public QueryLiteral    origin_qliteral
    cdef public IdContainer     origin
    cdef public long            selectivity
    cdef public list variables
    

        
    cpdef debug(self):
        rasqal_triple_print(<rasqal_triple*>self.t, stdout)

    def as_tuple(self):
        return (self.s, self.p, self.o)

    def __getitem__(self, i):
        if i == 0:
            return self.s
        elif i == 1:
            return self.p
        elif i == 2:
            return self.o
        elif i == 3:
            return self.origin
        else:
            raise IndexError('index must be, 0,1,2 or 3 corresponding to S, P, O or ORIGIN')

    def __str__(self):
        return '< %s, %s, %s >'%(str(self.s), str(self.p), str(self.o))

    def __iter__(self):
        self.__idx__ = 0
        return self

    def __next__(self):
        if self.__idx__ == 4:
            raise StopIteration
        else:
            item = None
            if self.__idx__ == 0:
                item = <object>self.s
            elif self.__idx__ == 1:
                item = <object>self.p
            elif self.__idx__ == 2:
                item = <object>self.o
            elif self.__idx__ == 3:
                item = <object>self.origin
            self.__idx__ += 1
            return item

    cdef inline __simple_selectivity_estimation__(self):
        return min([v.selectivity for v in self if type(v) is Variable])


    cpdef encode(self, sid, pid, oid, numeric=False):
        accesor = 'numeric_id' if numeric else 'hash_id'
        setattr(self.s, accesor, sid)
        setattr(self.p, accesor, pid)
        setattr(self.o, accesor, oid)

    def n3(self, withvars=True):
        def __n3__(itm):
            if itm:
                return itm.n3() if type(itm) is not Variable or (type(itm) is Variable and withvars) else None
        return (__n3__(self.s), __n3__(self.p), __n3__(self.o))
     
    def as_var_tuple(self):
        def __varname__(itm):
            if itm:
                return itm.name if type(itm) is Variable else None
        return (__varname__(self.s), __varname__(self.p), __varname__(self.o))
        
    def as_id_tuple(self, numeric=False):
        def __id__(itm):
            if itm:
                return itm.numeric_id if numeric else itm.hash_id
        return (__id__(self.s), __id__(self.p), __id__(self.o))

# factory method to construct - faster than python code with __init__ and __cinit__  
triples = {}
cdef Triple new_triple(rasqal_triple* t): 
    cdef long addr = <long>t
    #if triples.has_key(addr): return triples[addr]
    cdef Triple tp = Triple.__new__(Triple)
    tp.t        = t
    tp.__idx__  = 0
    tp.s_qliteral         = new_queryliteral(t.subject) if t.subject != NULL else None    
    tp.s                  = tp.s_qliteral.value
    tp.p_qliteral         = new_queryliteral(t.predicate) if t.predicate != NULL else None     
    tp.p                  = tp.p_qliteral.value
    tp.o_qliteral         = new_queryliteral(t.object) if t.object != NULL else None
    tp.o                  = tp.o_qliteral.value
    tp.origin_qliteral    = new_queryliteral(t.origin) if t.origin != NULL else None
    tp.origin             = tp.origin_qliteral.value if tp.origin_qliteral else None
    tp.variables = []
    if type(tp.s) == Variable: tp.variables.append(tp.s_qliteral.var_as_string())
    if type(tp.p) == Variable: tp.variables.append(tp.p_qliteral.var_as_string()) 
    if type(tp.o) == Variable: tp.variables.append(tp.o_qliteral.var_as_string())
    if type(tp.origin) == Variable: tp.variables.append(tp.origin_qliteral.var_as_string())
    triples[addr] = tp
    return tp
#-----------------------------------------------------------------------------------------------------------------------
# PREFIX
#-----------------------------------------------------------------------------------------------------------------------
cdef class Prefix:
    cdef rasqal_prefix*     p

    def __cinit__(self, p):
        self.p = <rasqal_prefix*>p

    property prefix:
        def __get__(self):
            return self.p.prefix if self.p.prefix != NULL else ''

    property uri:
        def __get__(self):
            return uri_to_str(self.p.uri) if self.p.uri != NULL else None

    cpdef debug(self):
        rasqal_prefix_print(<rasqal_prefix*>self.p, stdout)

    def __str__(self):
        return '(%s : %s)'%(self.prefix, self.uri)

    
#-----------------------------------------------------------------------------------------------------------------------
# GRAPH PATERN
#-----------------------------------------------------------------------------------------------------------------------
cdef class GraphPattern:
    cdef rasqal_graph_pattern*  gp
    cdef rasqal_query*          rq
    cdef int                    __idx__
    cdef public object          triples
    cdef public object          sub_graph_patterns
    cdef public object          flattened_triples
    
    def __iter__(self):
        return iter(self.triples)

    def __next__(self):
        if self.__idx__ == len(self.triples):
            raise StopIteration
        else:
            item = self.triples[self.__idx__]
            self.__idx__ += 1
            return item

    def __get_triples__(self):
        triples = []
        cdef rasqal_triple* t = NULL                                
        for i in count():
            t = rasqal_graph_pattern_get_triple(self.gp,i)
            if t == NULL: break
            triples.append(new_triple(t))
        return triples
        
    def __get_flattened_triples__(self):            
        cdef raptor_sequence* ts = rasqal_graph_pattern_get_flattened_triples(self.rq, self.gp)
        cdef int sz = 0
        if ts != NULL:
            sz = raptor_sequence_size(ts)
            return [new_triple(<rasqal_triple*>raptor_sequence_get_at(ts, i)) for i in xrange(sz)]
        return []

    def __get_subgraph_patterns__(self):
        cdef raptor_sequence* seq   = rasqal_graph_pattern_get_sub_graph_pattern_sequence(self.gp)
        cdef int sz = 0
        if seq != NULL:
            sz = raptor_sequence_size(seq)
            return [new_graphpattern(self.rq, <rasqal_graph_pattern*>raptor_sequence_get_at(seq, i)) for i in xrange(sz)]
        return []
        
    property operator:
        def __get__(self):
            return rasqal_graph_pattern_get_operator(self.gp)
    
    cpdef has_var(self, in_var):
        cdef Variable pvar = in_var   
        if rasqal_graph_pattern_variable_bound_in(self.gp, <rasqal_variable*> pvar.get_var()) == 0: return True
        return False 
        
    cpdef is_optional(self):
        return True if rasqal_graph_pattern_get_operator(self.gp) == RASQAL_GRAPH_PATTERN_OPERATOR_OPTIONAL else False

    cpdef is_basic(self):
        return True if rasqal_graph_pattern_get_operator(self.gp) == RASQAL_GRAPH_PATTERN_OPERATOR_BASIC else False

    cpdef is_union(self):
        return True if rasqal_graph_pattern_get_operator(self.gp) == RASQAL_GRAPH_PATTERN_OPERATOR_UNION else False

    cpdef is_group(self):
        return True if rasqal_graph_pattern_get_operator(self.gp) == RASQAL_GRAPH_PATTERN_OPERATOR_GROUP else False

    cpdef is_graph(self):
        return True if rasqal_graph_pattern_get_operator(self.gp) == RASQAL_GRAPH_PATTERN_OPERATOR_GRAPH else False

    cpdef is_filter(self):
        return True if rasqal_graph_pattern_get_operator(self.gp) == RASQAL_GRAPH_PATTERN_OPERATOR_FILTER else False

    cpdef is_service(self):
        return True if rasqal_graph_pattern_get_operator(self.gp) == RASQAL_GRAPH_PATTERN_OPERATOR_SERVICE else False
    
cdef GraphPattern new_graphpattern(rasqal_query* rq, rasqal_graph_pattern* gp):
    cdef GraphPattern grp = GraphPattern.__new__(GraphPattern)
    grp.gp                      = gp
    grp.rq                      = rq
    grp.__idx__                 = 0
    grp.triples                 = grp.__get_triples__()
    grp.sub_graph_patterns      = grp.__get_subgraph_patterns__()
    #grp.flattened_triples       = grp.__get_flattened_triples__()
    return grp
#-----------------------------------------------------------------------------------------------------------------------
# SEQUENCE
#-----------------------------------------------------------------------------------------------------------------------
cdef class Sequence:
    cdef raptor_sequence*   sq
    cdef int                __idx__

    def __cinit__(self, sq):
        self.sq = <raptor_sequence*>sq
        self.__idx__ = 0
        
    def __len__(self):
        return raptor_sequence_size(<raptor_sequence*>self.sq)

    def __setitem__(self, i, value):
        raptor_sequence_set_at(<raptor_sequence*>self.sq, i, <void*>value)

    def __delitem__(self, i):
        raptor_sequence_delete_at(<raptor_sequence*>self.sq, i)

    def __getitem__(self, i):
        return <object>raptor_sequence_get_at(<raptor_sequence*>self.sq, i)
        
    cpdef debug(self):
        raptor_sequence_print(<raptor_sequence*>self.sq, stdout)

    def __and__(self, other):
        raptor_sequence_join(<raptor_sequence*>self.sq, <raptor_sequence*>other)

    def shift(self, data):
        raptor_sequence_shift(<raptor_sequence*>self.sq, <void*>data)

    def unshift(self):
        return <object>raptor_sequence_unshift(<raptor_sequence*>self.sq)

    def pop(self):
        return <object>raptor_sequence_pop(<raptor_sequence*>self.sq)

    def push(self, data):
        raptor_sequence_push(<raptor_sequence*>self.sq, <void*>data)

    def __iter__(self):
        self.__idx__ = 0
        return self

    def __next__(self):
        if self.__idx__ == raptor_sequence_size(<raptor_sequence*>self.sq):
            raise StopIteration
        else:
            item = <object>raptor_sequence_get_at(<raptor_sequence*>self.sq, self.__idx__)
            self.__idx__ += 1
            return item

#-----------------------------------------------------------------------------------------------------------------------
#--- QUERY - KEEPS STATE (all are copies)
#-----------------------------------------------------------------------------------------------------------------------
cdef class Query:
    cdef RasqalWorld            w
    cdef rasqal_query*          rq
    cdef int                    __idx__
    cdef public object          vars
    cdef public object          bound_vars
    cdef public object          projections
    cdef public object          binding_vars
    cdef public object          prefixes
    cdef public object          triples
    cdef public GraphPattern    graph_pattern

    def __cinit__(self, query, world=None):
        self.w  = RasqalWorld() if not world else world
        self.__idx__ = 0
        self.rq = rasqal_new_query(self.w.rw, "sparql", NULL)
        rasqal_query_prepare(self.rq, <unsigned char*>query, NULL)
        
    def __init__(self, query, world=None):
        triples = {}
        self.triples        = self.__get_triples__()
        self.prefixes       = self.__get_prefixes__()
        self.graph_pattern  = self.__get_graph_pattern__()
        self.vars           = list(AllVarsIterator(<object>self.rq, None))
        self.bound_vars     = list(BoundVarsIterator(<object>self.rq, None))
        self.projections    = self.bound_vars
        self.binding_vars   = list(BindingsVarsIterator(<object>self.rq, None))

    def __dealloc__(self):
        rasqal_free_query(self.rq)

    cpdef debug(self):
        rasqal_query_print(self.rq, stdout)

    cpdef get_bindings_var(self, i):
        return Variable(<object>rasqal_query_get_bindings_variable(self.rq, i))

    cpdef get_var(self, i):
        return Variable(<object>rasqal_query_get_variable(self.rq, i))

    cpdef has_var(self, char* name):
        return True if rasqal_query_has_variable(self.rq, <unsigned char*>name) > 0 else False

    cpdef get_triple(self, i):
        return new_triple(rasqal_query_get_triple(self.rq, i))

    cpdef get_prefix(self, i):
        return Prefix(<object>rasqal_query_get_prefix(self.rq, i))

    def __get_triples__(self):
        cdef raptor_sequence* ts =  rasqal_query_get_triple_sequence(self.rq)
        cdef int sz = 0
        if ts != NULL:
            sz = raptor_sequence_size(ts)
            return [new_triple(rasqal_query_get_triple(self.rq, i)) for i in xrange(sz)]
        return []

    def __get_prefixes__(self):
        cdef raptor_sequence* ps =  rasqal_query_get_prefix_sequence(self.rq)
        cdef int sz = 0
        if ps != NULL:
            sz = raptor_sequence_size(ps)
            return [Prefix(<object>rasqal_query_get_prefix(self.rq, i)) for i in xrange(sz)]
        return []

    def __get_graph_pattern__(self):
        return new_graphpattern(self.rq, rasqal_query_get_query_graph_pattern(self.rq))

    property label:
        def __get__(self):
            return rasqal_query_get_label(self.rq)

    property limit:
        def __get__(self):
            return rasqal_query_get_limit(self.rq)

    property name:
        def __get__(self):
            return rasqal_query_get_name(self.rq)

    property offset:
        def __get__(self):
            return rasqal_query_get_offset(self.rq)

    property verb:
        def __get__(self):
            v = rasqal_query_get_verb(self.rq)
            if v == RASQAL_QUERY_VERB_UNKNOWN:
                return 'unknown'
            elif v == RASQAL_QUERY_VERB_SELECT:
                return 'select'
            elif v == RASQAL_QUERY_VERB_CONSTRUCT:
                return 'construct'
            elif v == RASQAL_QUERY_VERB_DESCRIBE:
                return 'describe'
            elif v == RASQAL_QUERY_VERB_ASK:
                return 'ask'
            elif v == RASQAL_QUERY_VERB_DELETE:
                return 'delete'
            elif v == RASQAL_QUERY_VERB_INSERT:
                return 'insert'
            elif v == RASQAL_QUERY_VERB_UPDATE:
                return 'update'

    def __getitem__(self, i):
        return self.triples[i]

    def __iter__(self):
        return iter(self.triples)

    def __str__(self):
        return '\n'.join([ 'TRIPLE: %s, %s, %s'%(t[0].n3(), t[1].n3(), t[2].n3()) for t in self ])


#-----------------------------------------------------------------------------------------------------------------------
#--- QUERY WRAPPER OVER RASQAL --- KEEPS NO STATE !
#-----------------------------------------------------------------------------------------------------------------------
cdef class QueryWrapper:
    cdef RasqalWorld w
    cdef rasqal_query* rq
    cdef int __idx__
    
    def __cinit__(self, query, world=None):
        self.w  = RasqalWorld() if not world else world
        self.__idx__ = 0

    def __init__(self, query, world=None):
        self.rq = rasqal_new_query(self.w.rw, "sparql", NULL)
        rasqal_query_prepare(self.rq, <unsigned char*>query, NULL)

    def __dealloc__(self):
        rasqal_free_query(self.rq)

    cpdef debug(self):
        rasqal_query_print(self.rq, stdout)

    property vars:
        def __get__(self):
            return AllVarsIterator(<object>self.rq, None)
            
    property bound_vars:
        def __get__(self):
            return BoundVarsIterator(<object>self.rq, None)

    property projections:
        def __get__(self):
            return BoundVarsIterator(<object>self.rq, None)

    property binding_vars:
        def __get__(self):
            return BindingsVarsIterator(<object>self.rq, None)

    cpdef get_bindings_var(self, i):
        return Variable(<object>rasqal_query_get_bindings_variable(self.rq, i))

    cpdef get_var(self, i):
        return Variable(<object>rasqal_query_get_variable(self.rq, i))

    cpdef has_var(self, char* name):
        return True if rasqal_query_has_variable(self.rq, <unsigned char*>name) > 0 else False

    cpdef get_triple(self, i):
        return new_triple(rasqal_query_get_triple(self.rq, i))

    cpdef get_prefix(self, i):
        return Prefix(<object>rasqal_query_get_prefix(self.rq, i))

    property prefixes:
        def __get__(self):
            cdef raptor_sequence* ps =  rasqal_query_get_prefix_sequence(self.rq)
            cdef int sz = 0
            if ps != NULL:
                sz = raptor_sequence_size(ps)
                return [Prefix(<object>rasqal_query_get_prefix(self.rq, i)) for i in xrange(sz)]
            return []

    property graph_pattern:
        def __get__(self):
            return new_graphpattern(self.rq, rasqal_query_get_query_graph_pattern(self.rq))

    property label:
        def __get__(self):
            return rasqal_query_get_label(self.rq)
    
    property limit:
        def __get__(self):
            return rasqal_query_get_limit(self.rq)
    
    property name:
        def __get__(self):
            return rasqal_query_get_name(self.rq)

    property offset:
        def __get__(self):
            return rasqal_query_get_offset(self.rq)

    property verb:
        def __get__(self):
            v = rasqal_query_get_verb(self.rq)
            if v == RASQAL_QUERY_VERB_UNKNOWN:
                return 'unknown'
            elif v == RASQAL_QUERY_VERB_SELECT:
                return 'select'
            elif v == RASQAL_QUERY_VERB_CONSTRUCT:
                return 'construct'
            elif v == RASQAL_QUERY_VERB_DESCRIBE:
                return 'describe'
            elif v == RASQAL_QUERY_VERB_ASK:
                return 'ask'
            elif v == RASQAL_QUERY_VERB_DELETE:
                return 'delete'
            elif v == RASQAL_QUERY_VERB_INSERT:
                return 'insert'
            elif v == RASQAL_QUERY_VERB_UPDATE:
                return 'update'

    def __getitem__(self, i):
        return new_triple(rasqal_query_get_triple(self.rq, i))

    def __iter__(self):
        return QueryTripleIterator(<object>self.rq, None)
    
    property triples:
        def __get__(self):
            cdef raptor_sequence* ts =  rasqal_query_get_triple_sequence(self.rq)
            cdef int sz = 0
            if ts != NULL:
                sz = raptor_sequence_size(ts)
                return [new_triple(rasqal_query_get_triple(self.rq, i)) for i in xrange(sz)]
            return []

    def __str__(self):
        return '\n'.join([ 'TRIPLE: %s, %s, %s'%(t[0].n3(), t[1].n3(), t[2].n3()) for t in self ])
