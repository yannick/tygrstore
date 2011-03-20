#!/bin/bash

for klass in index index_manager indexkc indextc kyoto_cabinet_stringstore query_engine stringstore helpers 
do
mv $klass.py  $klass.pyx
rm $klass.pyc
done

