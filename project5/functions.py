import json
import re
from pyspark import SparkContext


# A hack to avoid having to pass 'sc' around
dummyrdd = None
def setDefaultAnswer(rdd): 
	global dummyrdd
	dummyrdd = rdd

def task1(playRDD):
	new_RDD = playRDD.map(lambda line: (line.strip().split(" ")[0], (line.strip(), len(line.strip().split(" "))))).filter(lambda (x, (y,z)): (x, (y,z)) if (z > 10) else None)
        return new_RDD

def task2_flatmap(x):
	dict_list = x['laureates']
	new_list = []
	for i in range(0, len(dict_list)):
		new_list.append(dict_list[i]['surname'])
	return new_list

def task3_helper(x):
	dict_list = x['laureates']
	new_list = []
	for i in range(0, len(dict_list)):
		 new_list.append((x['category'], dict_list[i]['surname']))
	return new_list

def task3(nobelRDD):
	result1 = nobelRDD.map(json.loads).flatMap(task3_helper)
	result2 = result1.groupByKey().mapValues(list)
	return result2

def task4_helper(line):
	l = line.split(" ")
	date = l[3].split(':')
	new_date = date[0].replace("[", "")
	return (l[0], new_date)

def task4(logsRDD, l):
	set_l = set(l)
        RDD = logsRDD.map(task4_helper).groupByKey().mapValues(list)
	new_RDD = RDD.map(lambda (a,b): (a, set(b)))
	final_RDD = new_RDD.map(lambda (a,b): a if (set_l == set(b)) else None).filter(lambda x: x is not None)
	return final_RDD

def task5(bipartiteGraphRDD):
        RDD = bipartiteGraphRDD.groupByKey().mapValues(list)
	RDD1 = RDD.map(lambda (a,b): len(b))
	RDD2 = RDD1.map(lambda x: (x, 1))
	RDD3 = RDD2.reduceByKey(lambda v1, v2: v1 + v2)
	return RDD3
	
def task6_helper(line):
	l = line.split(" ")
	return (l[0], l[6])
	
def task6(logsRDD, day1, day2):
        RDD1 = logsRDD.filter(lambda line: day1 in line)
	RDD2 = logsRDD.filter(lambda line: day2 in line)
	
	new_RDD1 = RDD1.map(task6_helper)
	new_RDD2 = RDD2.map(task6_helper)
	
	RDD3 = new_RDD1.cogroup(new_RDD2)
	RDD4 = RDD3.map(lambda (a, (b,c)): (a, (list(b), list(c))))
	RDD5 = RDD4.filter(lambda (a, (b,c)): (a, (b,c)) if (b and c) else None)
	return RDD5

def task7_helper(x):
	dict_list = x['laureates']
	new_list = []
	j = 0
	k = 1
	for i in range(0, len(dict_list)):
		key = "motivation"
		if key in dict_list[i]:
			motiv_word_list = dict_list[i]['motivation'].split(" ")
			while k < len(motiv_word_list):
				new_list.append((motiv_word_list[j], motiv_word_list[k]))
				j = j + 1
				k = k + 1
		j = 0
		k = 1
	
	return new_list

def task7(nobelRDD):
        result1 = nobelRDD.map(json.loads).flatMap(task7_helper)
	result2 = result1.map(lambda (a, b): ((a, b), 1))
	result3 = result2.reduceByKey(lambda v1, v2: v1 + v2)
	return result3

def task8(bipartiteGraphRDD, currentMatching):

	# Find unmatched users (users not in currentMatching)
	user_product_RDD = bipartiteGraphRDD.subtractByKey(currentMatching)
	# Turn above RDD from (user,product) to (product,user)
	product_user_RDD = user_product_RDD.map(lambda (user,product): (product,user))

	# Turn currentMatching from (user,product) to (product,user)
	flipped_graph = currentMatching.map(lambda (user,product): (product,user))
	
	# Find the products in product_user_RDD not matched in flipped_graph ((product,user) version of currentMatching)
	new_product_user_RDD = product_user_RDD.subtractByKey(flipped_graph)
	# Flip above RDD back to original ordering (ie (user,product))
	new_user_product_RDD = new_product_user_RDD.map(lambda (product,user): (user,product))

	# For each user, take the minimun product (according to string alphabetical ordering)
	temp_user_RDD = new_user_product_RDD.reduceByKey(lambda v1, v2: min(v1,v2))
	# Flip above RDD around and find the minimum user for each product
	temp_product_RDD = temp_user_RDD.map(lambda (user,product): (product,user))
	temp_product_RDD2 = temp_product_RDD.reduceByKey(lambda v1,v2: min(v1,v2))
	
	# Flip back around to original ordering ((user,product)) and return final_RDD
	final_RDD = temp_product_RDD2.map(lambda (product,user): (user,product))
	
	return final_RDD
				
