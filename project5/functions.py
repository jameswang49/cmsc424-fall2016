import json
import re
from pyspark import SparkContext

# A hack to avoid having to pass 'sc' around
dummyrdd = None
def setDefaultAnswer(rdd): 
	global dummyrdd
	dummyrdd = rdd

def task1(playRDD):
	
	new_RDD = playRDD.map(lambda line: (line.split(" ")[0], (line, len(line)))).filter(lambda (x, (y,z)): (x, (y,z)) if z > 10 )
        return new_RDD

def task2_flatmap(x):
        return []        

def task3(nobelRDD):
        return dummyrdd

def task4(logsRDD, l):
        return dummyrdd

def task5(bipartiteGraphRDD):
        return dummyrdd

def task6(logsRDD, day1, day2):
        return dummyrdd


def task7(nobelRDD):
        return dummyrdd

def task8(bipartiteGraphRDD, currentMatching):
        return dummyrdd
