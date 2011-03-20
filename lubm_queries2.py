queries = {}
prefix = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
"""  

queries["lq1"] = ''' 
SELECT ?department WHERE 
{
        ?researchGroups ub:subOrganizationOf ?department .
        ?department ub:name '"Department1"' . } LIMIT 100'''     

queries["lq2"] = '''
SELECT ?mail ?phone ?doctor WHERE 
{       
        ?professor ub:name '"FullProfessor1"' .
        ?professor ub:emailAddress ?mail .
        ?professor ub:telephone ?phone . 
        ?professor ub:doctoralDegreeFrom ?doctor .
         }'''

queries["lq3"] = '''
SELECT ?studentName ?courseName WHERE {
     ?student ub:takesCourse ?course .
     ?course ub:name ?courseName .
     ?student  ub:name ?studentName .
     ?student ub:memberOf <http://www.Department1.University0.edu> . }'''   

queries["lq4"] = '''
SELECT ?publication ?author ?department ?university
WHERE {
        ?publication  ub:name '"Publication0"' .
        ?publication  ub:publicationAuthor ?author .
        ?author ub:worksFor ?department .
        ?department ub:subOrganizationOf ?university .
} LIMIT 100 '''


queries["lq5"] = '''
SELECT ?university ?name ?tel WHERE {
        ?student ub:advisor ?advisor .
        ?advisor ub:worksFor ?department .
        ?department ub:subOrganizationOf ?university .
        ?student ub:name ?name .
        ?student ub:telephone ?tel .
        ?student ub:takesCourse <http://www.Department1.University0.edu/GraduateCourse33> .
}LIMIT 100'''