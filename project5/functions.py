import json
import re
from pyspark import SparkContext

# A hack to avoid having to pass 'sc' around
dummyrdd = None
def setDefaultAnswer(rdd): 
	global dummyrdd
	dummyrdd = rdd

def task1(playRDD):
	new_RDD = playRDD.map(lambda line: (line.split(" ")[0], (line, len(line.split(" "))))).filter(lambda (x, (y,z)): (x, (y,z)) if (z > 10) else None)
        return new_RDD

def task2_flatmap(x):
	dict_list = x['laureates']
	new_list = []
	for i in range(0, len(dict_list)):
		new_list.append(dict_list[i]['surname'])
	return new_list

def ret_category_and_surnames(x):
	dict_list = x['laureates']
	new_list = []
	for i in range(0, len(dict_list)):
		 new_list.append((x['category'], dict_list[i]['surname']))
	return new_list

def task3(nobelRDD):
	result1 = nobelRDD.map(json.loads).flatMap(ret_category_and_surnames)
	result2 = result1.groupByKey().mapValues(list)
	return result2

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
