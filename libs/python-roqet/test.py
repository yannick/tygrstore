#!/usr/bin/python
import sys, roqet, pprint; pprint.pprint(roqet.sparql(file(sys.argv[1]).read()))
