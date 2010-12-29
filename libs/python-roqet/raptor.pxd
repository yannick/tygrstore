cdef extern from "raptor.h":
    # sequence
    ctypedef void* raptor_sequence "void *"
    int raptor_sequence_size(raptor_sequence *seq)
    void* raptor_sequence_get_at(raptor_sequence* seq, int idx)

    # uri
    ctypedef void *raptor_uri "void *"
    raptor_uri raptor_new_uri(unsigned char *uri_string)
    void raptor_free_uri(raptor_uri uri)
    unsigned char *raptor_uri_as_string(raptor_uri *uri)
    unsigned char *raptor_uri_filename_to_uri_string(char *filename)
