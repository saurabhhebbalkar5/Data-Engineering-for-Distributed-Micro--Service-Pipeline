# -*- coding: utf-8 -*-
"""
Created on Sat Feb 15 14:29:34 2020

@author: Saurabh
"""

import csv
import operator

sample = open('SortTest.csv', 'r')

csv1 = csv.reader(sample, delimiter=',')

sort = sorted (csv1, key = operator.itemgetter(1))

writer = csv.writer(open("Sorted.csv", 'w'))

for eachline in sort:
    writer.writerow(eachline)

writer.close()
    
    