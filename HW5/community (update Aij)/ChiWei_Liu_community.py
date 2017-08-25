import os, sys
from pyspark import SparkContext
from operator import add
import itertools
import time
import copy
import math
import Queue

tTotalStart = time.time()

def initialDict(dic,value):
	newDict = {}
	
	for key in dic:
		list1 = [0] * value
		for item in dic[key]:
			list1[item-1] = item
		newDict[key] = list1
	# print newDict
	return newDict

def binary_search(arr,x):
	lower = 0
	upper = len(arr)

	while lower < upper:
		mid = (lower + upper) // 2
		# print "mid: ",mid
		if arr[mid] > x:
			upper = mid
		elif arr[mid] < x:
			lower = mid + 1
		else:
			return mid

def checkCut(allMap,node1,node2):
	visited = {}
	# res = []
	que = Queue.Queue()
	que.put((node1,0))   # (node,depth,path)
	visited[node1] = (0,1)  # (depth,repeat times,path)
	# res.append(root)
	while not que.empty():
		node,depth = que.get()
		
			# print "node ",node
		for item in list(allMap[node]):
			if item > 0:
				if item == node2:
					return False
				if visited.get(item,0) == 0:
					# newPaths = []
					# for p in paths:
					# 	newPaths.append(p+[item])
					que.put((item,depth + 1))
					visited[item] = (depth + 1, 1)
					# res.append(item)
				
	return True

def findCommunity(allMap,root):
	visited = {}
	res = set()
	# que = []
	que = Queue.Queue()
	que.put((root,0))   # (node,depth,path)
	visited[root] = (0,1)  # (depth,repeat times,path)
	res.add(root)
	while not que.empty():
		node,depth = que.get()
		
		for item in list(allMap[node]):
			if item > 0:
				if visited.get(item,0) == 0:
					newPaths = []
					# for p in paths:
					# 	newPaths.append(p+[item])
					que.put((item,depth + 1))
					visited[item] = (depth + 1, 1)
					res.add(item)
				
	# print "res ",res
	return (res)

def bfs_test(allMap,root):
	visited = {}
	res = []
	# que = []
	que = Queue.Queue()
	que.put((root,0,[[root]]))   # (node,depth,path)
	visited[root] = (0,1,[[root]])  # (depth,repeat times,path)
	# res.append(root)
	while not que.empty():
		node,depth,paths = que.get()
		
			# print "node ",node
		for item in list(allMap[node]):
			if item > 0:
				if visited.get(item,0) == 0:
					newPaths = []
					for p in paths:
						newPaths.append(p+[item])
					que.put((item,depth + 1,newPaths))
					visited[item] = (depth + 1, 1,newPaths)
					res.append(item)
				else:
					# print "item ",item
					oldDepth,repeat,oldPath = visited[item]
					# print "oldDepth ",oldDepth
					# print "depth ",depth
					if oldDepth > depth:
						newPaths = []
						for p in paths:
							newPaths.append(p+[item])
						oldPath.extend(newPaths)

						visited[item] = (oldDepth,repeat + 1,oldPath)
	

	return(res)

def BFS_allEdges(allMap,root):
	visited = {}
	# depthDict = {}
	
	res = []
	que = Queue.Queue()
	que.put((root,0,[[root]]))   # (node,depth,path)
	visited[root] = (0,1,[[root]])  # (depth,repeat times,path)
	
	# res.append(root)
	
	while not que.empty():
		node,depth,paths = que.get()
		for item in list(allMap[node]):
			if visited.get(item,0) == 0:
				newPaths = []
				for p in paths:
					newPaths.append(p+[item])
				que.put((item,depth + 1,newPaths))
				visited[item] = (depth + 1, 1,newPaths)
					# visited[item] = (1,newPaths)
				res.append(item)
										
			else:
				oldDepth,repeat,oldPath = visited[item]
				if oldDepth > depth:
						# print "od ",oldDepth
						# print "d ",depth
					newPaths = []
					for p in paths:
						newPaths.append(p+[item])	
					oldPath.extend(newPaths)
					visited[item] = (oldDepth,repeat + 1,oldPath)

	print "root ",root
	yield (visited,res)

def getWeight(visited,target):
	# print visited
	# print target
	weight = {}
	# target = res
	for i in range(1,len(target)+1):
		d,rep,paths = visited[target[-i]]
		for p in paths:
			nodeVisitTimes = 1
			if len(p) > 1:
				_,nodeVisitTimes,_ = visited[p[-2]]
			for j in range(len(p)-1):
				
				if weight.get((p[j],p[j+1]),0) == 0:
					weight[(p[j],p[j+1])] = (float(1)/float(rep))/nodeVisitTimes
				else:
					weight[(p[j],p[j+1])] += (float(1)/float(rep))/nodeVisitTimes
	# print weight
	# print weight.items()
	yield weight.items()

def getWeight2(visited,target):
	weight = {}
	points = {}
	for i in range(1,len(target)+1):
		item = target[-i]
		_,rep,paths = visited[item]
		parent = set()
		for p in paths:
			parent.add(p[-2])
		
		newPoint = float(1 + points.get(item,0)) / float(rep)
		for n in parent:
			# newPoint = float(1 + points.get(item,0)) / float(rep)
			if n < item:
				weight[(n,item)] = newPoint
			else:
				weight[(item,n)] = newPoint
			if points.get(n,-1) == -1:
				points[n] = newPoint
			else:
				points[n] += newPoint
	# print "w ",weight
	yield weight.items()

def getAvg(key,value):

	avg = float(sum(value)) / float(2)

	yield (key,avg)

def cutEdge(allMap,node1,node2):
	list1 = allMap.get(node1)
	list1[node2-1] = 0
	allMap[node1] = list1
	list2 = allMap.get(node2)
	list2[node1-1] = 0
	allMap[node2] = list2

	return allMap

def buildK(allMap):
	k = {}
	for item in allMap:
		value = allMap[item]
		k[item] = len(value)
	return k

def countModualrity2(allMap,cutOrder,community,m):
	k = buildK(allMap)
	allMap = initialDict(allMap,671)
	# print "k ",k
	# print "allMap ",allMap
	res = []
	# cnt = 0
	for cutPair in cutOrder:
		node1,node2 = cutPair
		cutEdge(allMap,node1,node2)
		# print "allMap ",allMap
		# cnt += 1
		if checkCut(allMap,node1,node2):
		# community = []
			nc = []
			for comm in community:
				if node1 not in comm:
					nc.append(comm)
			nc.append(findCommunity(allMap,node1))
			nc.append(findCommunity(allMap,node2))
			community = nc
			# print community
		else:
			continue
		Q = 0
		# count all modularity
		for com in community:
			# com = list(com)
			print "com ",com
			# count modularity between nodes
			for item1 in com:
				list1 = allMap.get(item1)
				for item2 in com:
					if item1 < item2:
						Aij = 0
						if list1[item2-1] == item2:
							Aij = 1
						Q += Aij - (float(k[item1] * k[item2]) / (2 * m))
		print "final Q ",Q,"num ",len(community)
		res.append((Q,len(community),community))

		# 	for idx1 in range(len(com)-1):
		# 		item1 = com[idx1]
		# 		list1 = allMap.get(item1)

		# 		for idx2 in range(idx1+1,len(com)):
		# 			item2 = com[idx2]
		# 			# print "item1 ",item1
		# 			# print "item2 ",item2
		# 			Aij = 0
		# 			if list1[item2-1] == item2:
		# 				Aij = 1

		# 			Q += Aij - (float(k[item1] * k[item2]) / (2 * m))
		# 			# print "Q ", Q
		# print "final Q ",Q,"num ",len(community)
		# res.append((Q,len(community),community))

	# print res
	return res

def countModualrity(originalMap,cutOrder,community,m):
	k = buildK(originalMap)
	allMap = initialDict(originalMap,671)
	oriMap = initialDict(originalMap,671)
	# print "k ",k
	# print "allMap ",allMap
	# print "originalMap ",originalMap
	# print community
	res = []
	# cnt = 0
	##########
	Q = 0
		# count all modularity
	for com in community:
		com = list(com)
		com = sorted(com)
			# print "com ",com
			# count modularity between nodes
		for idx1 in range(len(com)-1):
			item1 = com[idx1]
			list1 = allMap.get(item1)

			for idx2 in range(idx1+1,len(com)):
				item2 = com[idx2]
					
				Aij = 0
				if list1[item2-1] == item2:
					Aij = 1

				Q += (Aij - (float(k[item1] * k[item2]) / (2 * m))) / (2 * m)
	print "Q ",Q,"num ",len(community)
	res.append(((Q,len(community)),community))
	###########


	for cutPair in cutOrder:
		node1,node2 = cutPair
		cutEdge(allMap,node1,node2)
		# print "allMap ",allMap
		# cnt += 1
		if checkCut(allMap,node1,node2):
		# community = []
			nc = []
			for comm in community:
				if node1 not in comm:
					nc.append(comm)
			nc.append(findCommunity(allMap,node1))
			nc.append(findCommunity(allMap,node2))
			community = nc
			# print "nc ",community
		else:
			continue
		Q = 0
		# count all modularity
		for com in community:
			com = list(com)
			com = sorted(com)
			# print "com ",com
			# count modularity between nodes
			for idx1 in range(len(com)-1):
				item1 = com[idx1]
				list1 = allMap.get(item1)

				for idx2 in range(idx1+1,len(com)):
					item2 = com[idx2]
					
					Aij = 0
					if list1[item2-1] == item2:
						Aij = 1

					Q += (Aij - (float(k[item1] * k[item2]) / (2 * m))) / (2 * m)
					# print "Q ", Q
		print "Q ",Q,"num ",len(community)
		res.append(((Q,len(community)),community))
	#print oriMap
	return res

INPUT_CSV = sys.argv[1]
OUTPUT_TXT = sys.argv[2]


sc = SparkContext(appName="ChiWei_Liu_hw5")
data = sc.textFile(INPUT_CSV,None,False)
dataHeader = data.first()
data = data.filter(lambda x: x!= dataHeader)#.map(lambda x: x.split(','))
data = data.map(lambda x: x.split(',')).map(lambda x: (int(x[0]),int(x[1]))).groupByKey().sortByKey(True)
allPair = data.cartesian(data).filter(lambda user: user[0][0] != user[1][0])
commonPair = allPair.map(lambda x: ((x[0][0],x[1][0]),set(x[0][1]).intersection(set(x[1][1])))).filter(lambda x: len(x[1]) >=3 )
commonPair = commonPair.map(lambda user: (user[0][0],user[0][1])).groupByKey()#.filter(lambda user: user[0] < user[1]).groupByKey()#.sortByKey(True)
# out = commonPair.collect()

# for v in sorted(out):
# 	print '%s,%s' % (v[0],list(v[1]))

# print out

########
allMap = commonPair.collectAsMap()
allEdges = commonPair.flatMap(lambda x: BFS_allEdges(allMap,x[0]))#.flatMap(lambda x: x)
edg = allEdges.flatMap(lambda x: getWeight2(x[0],x[1])).flatMap(lambda x: x).groupByKey().flatMap(lambda x: getAvg(x[0],x[1]))
m = edg.count()
cutOrder = edg.map(lambda x: (x[1],x[0])).sortByKey(False).map(lambda x: x[1]).collect()
community = []
firstCom = set()
for i in range(1,672):
	firstCom.add(i)
community.append(firstCom)
result = countModualrity(allMap,cutOrder,community,m)
communities = sc.parallelize(result)
communities = communities.sortByKey(True).collect()
ans = communities[-1]
res = []
for item in ans[1]:
	res.append(sorted(item))


# between = edg.collect()

# result = sorted(between)
fileToOpen = open(OUTPUT_TXT, 'w')
for v in sorted(res):
	fileToOpen.write("%s" % (v) + "\n") 
fileToOpen.close()
########

# out = allEdges.collect()
# print out
#allMap = {1:[2,3],2:[4,5],3:[6],4:[7,8],5:[8],6:[9,10],7:[],8:[11],9:[11],10:[11],11:[]}
#allMap = {1:[2,3],2:[4,5],3:[6],4:[7,8],5:[8],6:[],7:[],8:[]}



# k = {1:2,2:3,3:2,4:3,5:2,6:1,7:4,8:2}
# allMap = initialDict(allMap,8)

# allEdges = bfs_test(allMap,1)#.flatMap(lambda x: x).collect()
# print len(allEdges)

# allMap = {1:[2,3],2:[1,4,5],3:[1,6],4:[2,7,8],5:[2,8],6:[3],7:[4],8:[4,5]}
# cutOrder = [(1,3),(4,7),(4,8),(5,8),(2,5),(1,2),(3,6)]
#~~~~~~
# allMap = {1:[2,3],2:[1,3],3:[1,2,4],4:[3,5,6,7],5:[4,7],6:[4,7],7:[4,5,6]}
# cutOrder = [(3,4),(4,5),(4,7),(5,7),(2,3),(1,2)]
# m = 9
# community = [set([1,2,3,4,5,6,7])]
# countModualrity(allMap,cutOrder,community,m)
#~~~~~~~~




tTotalEnd = time.time()
print "It cost %f sec" % (tTotalEnd - tTotalStart)
# for v in out:
# 	print '%s,%s' % (v[0],list(v[1]))
