from raptor cimport *
from rasqal cimport *

def execute(source_uri, unsigned char *query_string):
    cdef rasqal_query rq
    cdef rasqal_query_results results
    cdef raptor_uri src_uri
    
    rasqal_init()
    rq = rasqal_new_query("sparql", NULL)
    
    src_uri = raptor_new_uri(raptor_uri_filename_to_uri_string(source_uri))
    rasqal_query_add_data_graph(<rasqal_query>rq, <raptor_uri>src_uri, <raptor_uri>NULL, 2)
    rasqal_query_prepare(<rasqal_query>rq, query_string, <raptor_uri>src_uri)
    raptor_free_uri(<raptor_uri>src_uri)
    
    results = rasqal_query_execute(<rasqal_query>rq)
    if rasqal_query_results_is_bindings(<rasqal_query_results>results):
        c = 0
        while not rasqal_query_results_finished(<rasqal_query_results>results):
            c += 1
            rasqal_query_results_next(<rasqal_query_results>results)
        print "found", c, "results"
    rasqal_free_query(<rasqal_query>rq)
    rasqal_finish()

cdef _conv_literal(rasqal_literal *l):
    cdef rasqal_variable *v
    if l.type == 2:
        return ('uri', rasqal_literal_as_string(l))
    elif l.type == 3:
        return ('literal', rasqal_literal_as_string(l))
    elif l.type == 12:
        v = rasqal_literal_as_variable(l)
        return ('var', v.name)

cdef _conv_expr_arg(rasqal_expression *arg):
    if <void*>arg.literal!=NULL:
        return _conv_literal(arg.literal)
    else:
        return _conv_expr(arg)

cdef _conv_expr(rasqal_expression *expr):
    if expr.op == 1:   return ('&&', _conv_expr_arg(expr.arg1), _conv_expr_arg(expr.arg2))
    elif expr.op == 2: return ('||',  _conv_expr_arg(expr.arg1), _conv_expr_arg(expr.arg2))
    elif expr.op == 3: return ('=',   _conv_expr_arg(expr.arg1), _conv_expr_arg(expr.arg2))
    elif expr.op == 4: return ('!=',  _conv_expr_arg(expr.arg1), _conv_expr_arg(expr.arg2))
    elif expr.op == 5: return ('<',   _conv_expr_arg(expr.arg1), _conv_expr_arg(expr.arg2))
    elif expr.op == 6: return ('>',   _conv_expr_arg(expr.arg1), _conv_expr_arg(expr.arg2))
    elif expr.op == 7: return ('<=',  _conv_expr_arg(expr.arg1), _conv_expr_arg(expr.arg2))
    elif expr.op == 8: return ('>=',  _conv_expr_arg(expr.arg1), _conv_expr_arg(expr.arg2))

cdef _walk_graph_pattern(rasqal_graph_pattern gp):
    cdef int i = 0
    cdef rasqal_triple *t
    cdef rasqal_expression *expr
    r = {
        'operator': rasqal_graph_pattern_operator_as_string(rasqal_graph_pattern_get_operator(gp)),
    }
    triples, external = [], {}
    while 1:
        t = rasqal_graph_pattern_get_triple(gp, i)
        if <void*>t == NULL: break
        s = _conv_literal(t.subject)
        p = _conv_literal(t.predicate)
        o = _conv_literal(t.object)
        if t.origin:
            why = _conv_literal(t.origin)
            if why: why = why[1]
            if not why in external:
                external[why] = [(s, p, o)]
            else:
                external[why].append((s, p, o))
        elif s and p and o:
            triples.append((s, p, o))
        i += 1
    if triples:
        r['triples'] = triples
    if external:
        r['external'] = external

    i = 0
    constraints = []
    while 1:
        expr = rasqal_graph_pattern_get_constraint(gp, i)
        if <void*>expr==NULL: break
        n = _conv_expr(expr)
        if n:
            constraints.append(n)
        i += 1
    if len(constraints) > 0:
        r['constraints'] = constraints

    i = 0
    patterns = []
    cdef rasqal_graph_pattern p0
    while 1:
        p0 = rasqal_graph_pattern_get_sub_graph_pattern(gp, i)
        if <void*>p0==NULL: break
        r0 = _walk_graph_pattern(p0)
        if len(r0) > 0:
            patterns.append(r0)
        i += 1
    if len(patterns) > 0:
        r['patterns'] = patterns
    return r

def sparql(unsigned char *query_string):
    cdef rasqal_query rq
    cdef raptor_uri base_uri
    
    rasqal_init()
    rq = rasqal_new_query("sparql", NULL)
    base_uri = raptor_new_uri("http://null/")
    rasqal_query_prepare(<rasqal_query>rq, query_string, <raptor_uri>base_uri)

    r = {
        'verb': rasqal_query_verb_as_string(rasqal_query_get_verb(rq)),
        'limit': rasqal_query_get_limit(rq),
    }

    cdef int i = 0
    cdef rasqal_variable *v
    variables = []
    while 1:
        v = rasqal_query_get_variable(rq, i)
        if <void*>v == NULL: break
        variables.append(v.name)
        i += 1
    r['variables'] = variables

    cdef rasqal_graph_pattern gp = rasqal_query_get_query_graph_pattern(rq)
    r['pattern'] = _walk_graph_pattern(gp)

    i = 0
    cdef rasqal_expression *ex = NULL
    order_conditions = []
    while 1:
        ex = rasqal_query_get_order_condition(rq, i)
        if <void*>ex == NULL: break
        if <void*>ex.arg1!=NULL and <void*>ex.arg1.literal!=NULL: order_conditions.append(_conv_literal(ex.arg1.literal))
        if <void*>ex.arg2!=NULL and <void*>ex.arg2.literal!=NULL: order_conditions.append(_conv_literal(ex.arg2.literal))
        if <void*>ex.arg3!=NULL and <void*>ex.arg3.literal!=NULL: order_conditions.append(_conv_literal(ex.arg3.literal))
        i += 1
    r['order_by'] = order_conditions

    raptor_free_uri(<raptor_uri>base_uri)
    rasqal_free_query(<rasqal_query>rq)
    rasqal_finish()

    return r
