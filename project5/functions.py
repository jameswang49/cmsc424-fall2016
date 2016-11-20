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
	# Initialize a temp RDD
	temp_RDD = bipartiteGraphRDD
	new_RDD = currentMatching
	first_iteration = 0
	
	for i in range(1, 292):
		# Find users unmatched in currentMatching
		current_user = "user%d" % (i)
		user_list = currentMatching.lookup(current_user)
		
		# If the user is not in currentMatching, find the products connected to that user.....
		if not user_list:
			user_product_RDD = bipartiteGraphRDD.filter(lambda (user,product): user == current_user)
			# Make the product the key and the user the value....
			product_user_RDD = user_product_RDD.map(lambda (user,product): (product,user))
			# Make a list of the above RDD
			product_user_RDD_list = product_user_RDD.collect()
			
			# Then find those products unmatched in currentMatching (if currentMatching is not empty)
			if not currentMatching.isEmpty():
				flipped_graph = currentMatching.map(lambda (user,product): (product,user))  # change ordering to (product, user)
				
				for (p,u) in product_user_RDD_list:
					match_list = flipped_graph.lookup(p)
					
					# If there's a matching product in currentMatching, filter tuple out of bipartiteGraphRDD
					if match_list:
						for j in range (0, len(match_list)):
							if (first_iteration == 0):
								temp_RDD = bipartiteGraphRDD.filter(lambda (user,product): product != p and user != u)
								first_iteration = 1
							else:
								temp_RDD = temp_RDD.filter(lambda (user,product): product != p and user != u)
							
				# Of the user-products in the unmatched RDD (temp), find the one with the minimum product
				# ie, the string value is the least of all the other string values. 
				new_RDD = temp_RDD.reduceByKey(lambda v1, v2: v1 if (v1 < v2) else v2)
				
			# But if currentMatching is empty, then there are no matching products in it and we can skip to the last step
			else:
				new_RDD = user_product_RDD.reduceByKey(lambda v1, v2: v1 if (v1 < v2) else v2)
		
		# If the user has a match in currentMatching, filter out all of those users from the graph
		else: 
			new_RDD = bipartiteGraphRDD.filter(lambda (user,product): user != current_user)
	
	return new_RDD
				
