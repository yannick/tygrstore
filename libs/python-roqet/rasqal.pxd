from raptor cimport *

cdef extern from "rasqal.h":
    void rasqal_init()
    void rasqal_finish()

    ctypedef void *rasqal_query "void *"
    ctypedef void *rasqal_query_results "void *"
    ctypedef int rasqal_query_verb
    ctypedef void *rasqal_graph_pattern "void *"
    ctypedef int rasqal_graph_pattern_operator
    ctypedef int rasqal_variable_type
    ctypedef int rasqal_literal_type
    ctypedef int rasqal_op

    ctypedef struct rasqal_prefix:
        unsigned char *prefix
        raptor_uri *uri
        int declared
        int depth

    cdef struct rasqal_literal_s:
        int usage
        rasqal_literal_type type
        unsigned char* string
        unsigned int string_len
        # more C... UNION, etc.
    ctypedef rasqal_literal_s rasqal_literal

    cdef struct rasqal_expression_s:
        int usage
        rasqal_op op
        rasqal_expression_s *arg1
        rasqal_expression_s *arg2
        rasqal_expression_s *arg3
        rasqal_literal *literal
        unsigned char *value
        raptor_uri *name
        raptor_sequence *args
    ctypedef rasqal_expression_s rasqal_expression

    ctypedef struct rasqal_variable:
        unsigned char *name
        rasqal_literal *value
        int offset
        rasqal_variable_type type
        rasqal_expression_s *expression

    ctypedef struct rasqal_triple:
        rasqal_literal *subject
        rasqal_literal *predicate
        rasqal_literal *object
        rasqal_literal *origin
        unsigned int flags

    # query
    rasqal_query rasqal_new_query(char *name, char *uri)
    int rasqal_query_add_data_graph(rasqal_query query, raptor_uri uri, raptor_uri name_uri, int flags)
    int rasqal_query_prepare(rasqal_query query, unsigned char *query_string, raptor_uri base_uri)
    rasqal_query_results rasqal_query_execute(rasqal_query query)
    void rasqal_free_query(rasqal_query query)

    rasqal_query_verb rasqal_query_get_verb(rasqal_query query)
    char *rasqal_query_verb_as_string(rasqal_query_verb verb)
    int rasqal_query_get_limit(rasqal_query query)
    int rasqal_query_get_offset(rasqal_query query)

    #raptor_sequence rasqal_query_get_bound_variable_sequence(rasqal_query query);
    #raptor_sequence rasqal_query_get_all_variable_sequence(rasqal_query query)
    rasqal_variable *rasqal_query_get_variable(rasqal_query query, int idx)
    rasqal_triple *rasqal_query_get_triple(rasqal_query query, int idx)
    rasqal_prefix *rasqal_query_get_prefix(rasqal_query query, int idx)
    rasqal_expression *rasqal_query_get_order_condition(rasqal_query query, int idx)
    #void rasqal_query_graph_pattern_visit(rasqal_query query, rasqal_graph_pattern_visit_fn visit_fn, void* data)

    # patterns
    rasqal_graph_pattern rasqal_query_get_query_graph_pattern(rasqal_query query)
    rasqal_triple *rasqal_graph_pattern_get_triple(rasqal_graph_pattern graph_pattern, int idx)
    rasqal_graph_pattern rasqal_graph_pattern_get_sub_graph_pattern(rasqal_graph_pattern graph_pattern, int idx)
    rasqal_graph_pattern_operator rasqal_graph_pattern_get_operator(rasqal_graph_pattern graph_pattern)
    char* rasqal_graph_pattern_operator_as_string(rasqal_graph_pattern_operator op)
    rasqal_expression *rasqal_graph_pattern_get_constraint(rasqal_graph_pattern gp, int idx)

    # literals
    #rasqal_literal *rasqal_literal_as_node(rasqal_literal *l)
    rasqal_variable *rasqal_literal_as_variable(rasqal_literal *l)
    unsigned char *rasqal_literal_as_string(rasqal_literal *l)
    #raptor_uri *rasqal_literal_datatype(rasqal_literal *l)

    # results
    void rasqal_free_query_results(rasqal_query_results query_results)
    int rasqal_query_results_is_bindings(rasqal_query_results query_results)
    int rasqal_query_results_get_count(rasqal_query_results query_results)
    int rasqal_query_results_next(rasqal_query_results query_results)
    int rasqal_query_results_finished(rasqal_query_results query_results)
