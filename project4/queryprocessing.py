import math
from disk_relations import *

# We will implement our operators using the iterator interface
# discussed in Section 12.7.2.1
class Operator:
	def init(self):
		return
	def get_next(self):
		return
	def close(self):
		return

# We will only support equality predicate for now
# This class denotes a predicate, e.g. "a = 5" (attribute will be a, and value will be 5), 
# and it supports a single method: "satisfiedBy"
class Predicate:
	def __init__(self, attribute, value):
		self.attribute = attribute
		self.value = value
	def satisfiedBy(self, t):
		return t.getAttribute(self.attribute) == self.value

# The simplest operator is SequentialScan, whose get_next() simply returns all the tuples one by one
class SequentialScan(Operator):
	def __init__(self, relation, predicate = None):
		self.relation = relation
		self.predicate = predicate

	# Typically the init() here would open the appropriate file, etc. In our 
	# simple implementation, we don't need to do anything, especially when we
	# use "yield"
	def init(self):
		return

	# This is really simplified because of "yield", which allows us to return a value, 
	# and then continue where we left off when the next call comes
	def get_next(self):
		for i in range(0, len(self.relation.blocks)):
			b = self.relation.blocks[i]
			if Globals.printBlockAccesses:
				print "Retrieving " + str(b)
			for j in range(0, len(self.relation.blocks[i].tuples)):
				t = b.tuples[j]
				if t is not None and (self.predicate is None or self.predicate.satisfiedBy(t)):
					yield t

	# Typically you would close any open files etc.
	def close(self):
		return

# Next we implement the Nested Loops Join, which has two other operators as inputs: left_child and right_child
# We will only support Equality joins and Left Outer Joins -- Right Outer Joins are tricky to do using NestedLoops
class NestedLoopsJoin(Operator):
	INNER_JOIN = 0
	LEFT_OUTER_JOIN = 1
	def __init__(self, left_child, right_child, left_attribute, right_attribute, jointype = INNER_JOIN):
		self.left_child = left_child
		self.right_child = right_child
		self.left_attribute = left_attribute
		self.right_attribute = right_attribute
		self.jointype = jointype

	# Call init() on the two children
	def init(self):
		self.left_child.init()
		self.right_child.init()

	# Again using "yield" greatly simplifies writing of this code -- otherwise we would have 
	# to keep track of current pointers etc
	def get_next(self):
		for l in self.left_child.get_next():
			foundAMatch = False
			for r in self.right_child.get_next():
				if l.getAttribute(self.left_attribute) == r.getAttribute(self.right_attribute):
					foundAMatch = True
					output = list(l.t)
					output.extend(list(r.t))
					yield Tuple(None, output)
			# If we are doing LEFT_OUTER_JOIN, we need to output a tuple if there is no match
			if self.jointype == NestedLoopsJoin.LEFT_OUTER_JOIN and not foundAMatch:
				output = list(l.t)
				for i in range(0, len(self.right_child.relation.schema)):
					output.append("NULL")
				yield Tuple(None, output)
			# NOTE: RIGHT_OUTER_JOIN is not easy to do with NestedLoopsJoin, so you would swap the children
			# if you wanted to do that

	# Typically you would close any open files etc.
	def close(self):
		return

# We will only support Equality joins
# Inner Hash Joins are very simple to implement, especially if you assume that the right relation fits in memory
# We start by loading the tuples from the right input into a hash table, and then for each tuple in the second
# input (left input) we look up matches
class HashJoin(Operator):
	INNER_JOIN = 0
	def __init__(self, left_child, right_child, left_attribute, right_attribute, jointype):
		self.left_child = left_child
		self.right_child = right_child
		self.left_attribute = left_attribute
		self.right_attribute = right_attribute
		self.jointype = jointype

	# Call init() on the two children
	def init(self):
		self.left_child.init()
		self.right_child.init()

	# We will use Python "dict" data structure as the hash table
	def get_next(self):
		if self.jointype == self.INNER_JOIN:
			# First, we load up all the tuples from the right input into the hash table
			hashtable = dict()
			for r in self.right_child.get_next():
				key = r.getAttribute(self.right_attribute)
				if key in hashtable:
					hashtable[r.getAttribute(self.right_attribute)].append(r)
				else: 
					hashtable[r.getAttribute(self.right_attribute)] = [r]
			# Then, for each tuple in the left input, we look for matches and output those
			# Using "yield" significantly simplifies this code
			for l in self.left_child.get_next():
				key = l.getAttribute(self.left_attribute)
				if key in hashtable:
					for r in hashtable[key]:
						output = list(l.t)
						output.extend(list(r.t))
						yield Tuple(None, output)

		else:
			raise ValueError("This should not happen")

	# Typically you would close any open files, deallocate hash tables etc.
	def close(self):
		self.left_child.close()
		self.right_child.close()
		return

class GroupByAggregate(Operator):
	COUNT = 0
	SUM = 1
	MAX = 2
	MIN = 3
	AVERAGE = 4
	MEDIAN = 5
	MODE = 6

	@staticmethod
	def initial_value(aggregate_function):
		initial_values = [0, 0, None, None, [], [], dict()]
		return initial_values[aggregate_function]

	@staticmethod
	def update_aggregate(aggregate_function, current_aggregate, new_value):
		if aggregate_function == GroupByAggregate.COUNT:
			return current_aggregate + 1
		
		elif aggregate_function == GroupByAggregate.SUM:
			return current_aggregate + int(new_value)
		
		elif aggregate_function == GroupByAggregate.MAX:
			if current_aggregate is None:
				return new_value
			else:
				return max(current_aggregate, new_value)
			
		elif aggregate_function == GroupByAggregate.MIN:
			if current_aggregate is None:
				return new_value
			else:
				return min(current_aggregate, new_value)
			
		elif aggregate_function == GroupByAggregate.AVERAGE:
			return new_value
			
		elif aggregate_function == GroupByAggregate.MEDIAN:
			return new_value
			
		elif aggregate_function == GroupByAggregate.MODE:
			return 1
		else:
			raise ValueError("No such aggregate")

	# Do any final computation that needs to be done 
	# For COUNT, SUM, MIN, MAX, we can just return what we have been computing
	# For the other three, we need to do something more
	@staticmethod
	def final_aggregate(aggregate_function, current_aggregate):
		if aggregate_function in [GroupByAggregate.COUNT, GroupByAggregate.SUM, GroupByAggregate.MIN, GroupByAggregate.MAX]:
			return current_aggregate 
		
		elif aggregate_function == GroupByAggregate.AVERAGE:
			num_elems = 0.0
			sum_value = 0.0
			for i in range(0, len(current_aggregate)):
				num_elems = num_elems + 1
				sum_value = sum_value + float(current_aggregate[i])
			return sum_value/num_elems
				
		elif aggregate_function == GroupByAggregate.MEDIAN:
			current_aggregate.sort()
			index_of_median = int(math.floor(len(current_aggregate)/2))
			return current_aggregate[index_of_median]
		
		elif aggregate_function == GroupByAggregate.MODE:
			largest_count = 0
			mode_value = 0
			for key, val in current_aggregate.iteritems():
				if val > largest_count:
					largest_count = int(val)
					mode_value = key
			return mode_value
		else:
			raise ValueError("No such aggregate")


	def __init__(self, child, aggregate_attribute, aggregate_function, group_by_attribute = None):
		self.child = child
		self.group_by_attribute = group_by_attribute
		self.aggregate_attribute = aggregate_attribute
		# The following should be between 0 and 3, as interpreted above
		self.aggregate_function = aggregate_function

	def init(self):
		self.child.init()

	def get_next(self):
		if self.group_by_attribute is None:
			# We first use initial_value() to set up an appropriate initial value for the aggregate, e.g., 0 for COUNT and SUM
			aggr = GroupByAggregate.initial_value(self.aggregate_function)

			# Then, for each input tuple: we update the aggregate appropriately
			for t in self.child.get_next():
				
				if self.aggregate_function == 4 or self.aggregate_function == 5:
					aggr.append(GroupByAggregate.update_aggregate(self.aggregate_function, aggr, t.getAttribute(self.aggregate_attribute)))	
					
				else:
					if self.aggregate_function == 6:
						if t.getAttribute(self.aggregate_attribute) not in aggr:
							aggr[t.getAttribute(self.aggregate_attribute)] = GroupByAggregate.update_aggregate(self.aggregate_function, aggr, t.getAttribute(self.aggregate_attribute))
							
						else:
							aggr[t.getAttribute(self.aggregate_attribute)] += GroupByAggregate.update_aggregate(self.aggregate_function, aggr, t.getAttribute(self.aggregate_attribute))
					else:
						aggr = GroupByAggregate.update_aggregate(self.aggregate_function, aggr, t.getAttribute(self.aggregate_attribute))

			# There is only one output here, but we must use "yield" since the "else" code needs to use "yield" (that code
			# may return multiple groups)
			yield Tuple(None, (GroupByAggregate.final_aggregate(self.aggregate_function, aggr)))
		else:
			# for each different value "v" of the group by attribute, we should return a 2-tuple "(v, aggr_value)",
			# where aggr_value is the value of the aggregate for the group of tuples corresponding to "v"
			
			# we will set up a 'dict' to keep track of all the groups
			aggrs = dict()

			for t in self.child.get_next():
				g_attr = t.getAttribute(self.group_by_attribute)

				# initialize if not already present in aggrs dictionary
				if g_attr not in aggrs:
					aggrs[g_attr] = GroupByAggregate.initial_value(self.aggregate_function)
					
				if self.aggregate_function == 4 or self.aggregate_function == 5:
					aggrs[g_attr].append(GroupByAggregate.update_aggregate(self.aggregate_function, aggrs[g_attr], t.getAttribute(self.aggregate_attribute)))		
				
				else:
					if self.aggregate_function == 6:
						if t.getAttribute(self.aggregate_attribute) not in aggrs[g_attr]:
							aggrs[g_attr][t.getAttribute(self.aggregate_attribute)] = GroupByAggregate.update_aggregate(self.aggregate_function, aggrs[g_attr], t.getAttribute(self.aggregate_attribute))
						else:
							aggrs[g_attr][t.getAttribute(self.aggregate_attribute)] += GroupByAggregate.update_aggregate(self.aggregate_function, aggrs[g_attr], t.getAttribute(self.aggregate_attribute))
					else: 
						aggrs[g_attr] = GroupByAggregate.update_aggregate(self.aggregate_function, aggrs[g_attr], t.getAttribute(self.aggregate_attribute))

			# now that the aggregate is compute, return one by one
			for g_attr in aggrs:
				yield Tuple(None, (g_attr, GroupByAggregate.final_aggregate(self.aggregate_function, aggrs[g_attr])))


class SortMergeJoin(Operator):
	INNER_JOIN = 0
	FULL_OUTER_JOIN = 1

	def __init__(self, left_child, right_child, left_attribute, right_attribute, jointype = INNER_JOIN):
		self.left_child = left_child
		self.right_child = right_child
		self.left_attribute = left_attribute
		self.right_attribute = right_attribute
		self.jointype = jointype

	# Call init() on the two children
	def init(self):
		self.left_child.init()
		self.right_child.init()

	# We assume that the two inputs are small enough to fit into memory, so there is no need to do external sort
	# We will load the two relations into arrays and use Python sort routines to sort them, and then merge, using 'yield' to simplify the code
	def get_next(self):
		left_input = [r for r in self.left_child.get_next()]
		right_input = [r for r in self.right_child.get_next()]

		left_input.sort(key=lambda t: t.getAttribute(self.left_attribute))
		right_input.sort(key=lambda t: t.getAttribute(self.right_attribute))

		if self.jointype == self.INNER_JOIN:
			ptr_l = 0
			ptr_r = 0

			while ptr_l < len(left_input) and ptr_r < len(right_input):
				set_L = [left_input[ptr_l]]
				l_attr = left_input[ptr_l].getAttribute(self.left_attribute) 

				ptr_l += 1
				while ptr_l < len(left_input):
					if left_input[ptr_l].getAttribute(self.left_attribute) == l_attr:
						set_L.append(left_input[ptr_l])
						ptr_l += 1
					else:
						break

				while ptr_r < len(right_input) and right_input[ptr_r].getAttribute(self.right_attribute) <= l_attr:
					if right_input[ptr_r].getAttribute(self.right_attribute) == l_attr:
						for l in set_L:
							output = list(l.t)
							output.extend(list(right_input[ptr_r].t))
							yield Tuple(None, output)
					ptr_r += 1

		elif self.jointype == self.FULL_OUTER_JOIN:
			ptr_l = 0
			ptr_r = 0
			
			ptr_l2 = 0
			ptr_r2 = 0
			found = 0
			
			left_schema_len = len(self.left_child.relation.schema)
			right_schema_len = len(self.right_child.relation.schema)
			

			while ptr_l < len(left_input) and ptr_r < len(right_input):
				found = 0
				set_L = [left_input[ptr_l]]
				l_attr = left_input[ptr_l].getAttribute(self.left_attribute) 

				ptr_l += 1
				while ptr_l < len(left_input):
					if left_input[ptr_l].getAttribute(self.left_attribute) == l_attr:
						set_L.append(left_input[ptr_l])
						ptr_l += 1
					else:
						break

				while ptr_r < len(right_input) and right_input[ptr_r].getAttribute(self.right_attribute) <= l_attr:
					if right_input[ptr_r].getAttribute(self.right_attribute) == l_attr:
						found = 1
						for l in set_L:
							output = list(l.t)
							output.extend(list(right_input[ptr_r].t))
							yield Tuple(None, output)
					ptr_r += 1
					
				if found == 0:
					output = list(l.t)
						for i in range (0, right_schema_len):
							output.append(None)
						yield Tuple(None, output)
						break
					
			'''	
			while ptr_l2 < len(left_input) and ptr_r2 < len(right_input):
				set_R = [right_input[ptr_r2]]
				r_attr = right_input[ptr_r2].getAttribute(self.right_attribute) 

				ptr_r2 += 1

				while ptr_l2 < len(left_input):
					if left_input[ptr_l2].getAttribute(self.left_attribute) > r_attr and found == 0:
						for r in set_R:
							output = list(right_input[ptr_r2].t)
							for i in range (0, left_schema_len):
								output.insert(0, None)
							yield Tuple(None, output)
							break
						
					elif left_input[ptr_l2].getAttribute(self.left_attribute) == r_attr:
						found = 1
						
					ptr_l2 += 1
				
				found = 0
			'''		
		else:
			raise ValueError("This should not happen")

	# Typically you would close any open files, deallocate hash tables etc.
	def close(self):
		self.left_child.close()
		self.right_child.close()

# You have to implement the Set Minus operator (also called EXCEPT in some cases)
# The input is two relations with identical schema, and the output is: left_child - right_child
# By default, SQL EXCEPT removes duplicates -- our implementation may or may not remove duplicates based on the flag 'keep_duplicates' -- you have to implement both
# Easiest way to implement this operator is through using a Hash Table, similarly to the Hash Join above
class SetMinus(Operator):
	def __init__(self, left_child, right_child, keep_duplicates = False):
		self.left_child = left_child
		self.right_child = right_child
		self.keep_duplicates = keep_duplicates

	# Call init() on the two children
	def init(self):
		self.left_child.init()
		self.right_child.init()

	# As above, use 'yield' to simplify writing this code
	def get_next(self):
		left_hashtable = dict()
		right_hashtable = dict()
			
		# Load right input tuples into right_hashtable. The value is the number of copies
		# of the tuple found in the relation
		for r in self.right_child.get_next():
			if r.t in right_hashtable:
				right_hashtable[r.t] += 1
			else:
				right_hashtable[r.t] = 1
			
		# Load left input tuples into left_hashtable. The value is the number of copies
		# of the tuple found in the relation
		for r in self.left_child.get_next():
			if r.t in left_hashtable:
				left_hashtable[r.t] += 1
			else:
				left_hashtable[r.t] = 1
					
		# If the length of the left_hashtable is 0 (empty relation), return None		
		if len(left_hashtable.items()) == 0:
			yield (None, None)
		
		# Otherwise iterate through each key in the left_hashtable. If the key is found in the right_hashtable
		# and keep_duplicates is set to True, subtract the value at right_hashtable[key] (key = tuple) from the value
		# at left_hashtable[key]. If the number (set_minus) is greater than 0, return that many copies of the tuple.
		# If keep_duplicates is set to False, only yield left_hashtable tuples (keys) that are not present in right_hashtable
		else : 
			for key, value in left_hashtable.items():
				if key in right_hashtable:
					if self.keep_duplicates is True:
						left_num_tuples = left_hashtable[key]
						right_num_tuples = right_hashtable[key]
						set_minus = left_num_tuples - right_num_tuples
							
						if set_minus > 0:
							for i in range(0, set_minus):
								yield Tuple(None, key)
				else:
					yield(None, key)
				
			
		
	# Typically you would close any open files etc.
	def close(self):
		return
