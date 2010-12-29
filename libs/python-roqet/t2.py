import roqet
def test():
    res = roqet.sparql("""PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX qs: <http://rdf.qwobl.com/schema#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
SELECT * WHERE {
    GRAPH <http://presbrey.mit.edu/foaf> {
        <http://presbrey.mit.edu/foaf#presbrey> foaf:interest ?interest
    }
    ?event ?p ?interest ;
           qs:class ?l_class .
    ?l_class rdfs:label "event"
}
""")
#    print res

if __name__=='__main__':
    from timeit import Timer
    t = Timer("test()", "from __main__ import test")
    print t.timeit(number=1000)
