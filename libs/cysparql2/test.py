import cysparql as sparql
qry = '''PREFIX foaf: <http://xmlns.com/foaf/0.1/>
SELECT ?name ?mbox ?crack ?sirname
WHERE  { { ?x foaf:name  ?name .
         ?x foaf:sirname  ?sirname .   
          OPTIONAL { ?x  foaf:mbox  ?mbox }
          OPTIONAL { ?x  foaf:crack ?crack }
          FILTER regex(?name, "Smith") }                   
          UNION { ?x foaf:lol  ?lol . }         
          
        }'''
                         
                         
q = sparql.Query(qry)
 
gp = q.graph_pattern

level1 = [i for i in gp.sub_graph_patterns]

level10 = [i for i in level1[0].sub_graph_patterns]                                                                
level11 = [i for i in level1[1].sub_graph_patterns]

for gp in level10:
    print ""