import os, sys
from pyspark import SparkContext
from operator import add
import itertools
import time
import copy
import math

tTotalStart = time.time()

def compareV(old, new):
	if new < old:
		return new
	else: 
		return old

def minHash(val,l):
	val = 1000
	for i in range(len(l)):
		if l[i] < val:
			val = l[i]
	return val

def minHash1(user,val,l):
	val = 1000
	if l[user - 1] < val:
		val = l[user - 1]
	return val

def findMinV(val):
	val = list(val)
	minV = val[0]
	for i in range(1,len(val)):
		if val[i] < minV:
			minV = val[i]
	return minV

def hash(data,f):
	h = data.map(lambda x: (int(x[0]),(int(x[1]),f[0])))
	h = h.map(lambda x: (x[0],(x[1][0],minHash1(x[0],x[1][1],f)))).map(lambda x: (x[1][0],x[1][1])).groupByKey().map(lambda x: (x[0],findMinV(list(x[1]))))
	return h

def combine(tup,val):
	l = []
	for i in range(len(tup)):
		l.append(tup[i])
	l.append(val)
	return tuple(l)

def hashMultipleTimes(data):
	matrix = data.map(lambda x: (int(x[0]),(int(x[1]),1))).groupByKey().sortByKey(True)
	f1 = matrix.map(lambda x: (x[0] + 1) % 671).collect()
	f2 = matrix.map(lambda x: (3 * x[0] + 1) % 671).collect()
	f3 = matrix.map(lambda x: (5 * x[0] + 1) % 671).collect()
	f4 = matrix.map(lambda x: (7 * x[0] + 1) % 671).collect()
	f5 = matrix.map(lambda x: (9 * x[0] + 1) % 671).collect()
	f6 = matrix.map(lambda x: (2 * x[0] + 1) % 671).collect()
	f7 = matrix.map(lambda x: (4 * x[0] + 1) % 671).collect()
	f8 = matrix.map(lambda x: (6 * x[0] + 1) % 671).collect()
	f9 = matrix.map(lambda x: (8 * x[0] + 1) % 671).collect()
	f10 = matrix.map(lambda x: (3 * x[0] + 2) % 671).collect()

	f11 = matrix.map(lambda x: (3 * x[0] + 5) % 671).collect()
	f12 = matrix.map(lambda x: (3 * x[0] + 7) % 671).collect()
	# f13 = matrix.map(lambda x: (3 * x[0] + 5) % 671).collect()
	# f14 = matrix.map(lambda x: (3 * x[0] + 7) % 671).collect()

	h1 = hash(data,f1)
	h2 = hash(data,f2)
	hTotal = h1.join(h2)
	h3 = hash(data,f3)
	# hTotal = hTotal.join(h3).map(lambda x: (x[0],(x[1][0][0],x[1][0][1],x[1][1])))
	hTotal = hTotal.join(h3).map(lambda x: (x[0],combine(x[1][0],x[1][1])))
	h4 = hash(data,f4)
	hTotal = hTotal.join(h4).map(lambda x: (x[0],combine(x[1][0],x[1][1])))
	h5 = hash(data,f5)
	hTotal = hTotal.join(h5).map(lambda x: (x[0],combine(x[1][0],x[1][1])))
	h6 = hash(data,f6)
	hTotal = hTotal.join(h6).map(lambda x: (x[0],combine(x[1][0],x[1][1])))
	h7 = hash(data,f7)
	hTotal = hTotal.join(h7).map(lambda x: (x[0],combine(x[1][0],x[1][1])))
	h8 = hash(data,f8)
	hTotal = hTotal.join(h8).map(lambda x: (x[0],combine(x[1][0],x[1][1])))

	h9 = hash(data,f9)
	hTotal = hTotal.join(h9).map(lambda x: (x[0],combine(x[1][0],x[1][1])))
	h10 = hash(data,f10)
	hTotal = hTotal.join(h10).map(lambda x: (x[0],combine(x[1][0],x[1][1])))
	h11 = hash(data,f11)
	hTotal = hTotal.join(h11).map(lambda x: (x[0],combine(x[1][0],x[1][1])))
	h12 = hash(data,f12)
	hTotal = hTotal.join(h12).map(lambda x: (x[0],combine(x[1][0],x[1][1])))

	# h13 = hash(data,f13)
	# hTotal = hTotal.join(h13).map(lambda x: (x[0],combine(x[1][0],x[1][1])))
	# h14 = hash(data,f14)
	# hTotal = hTotal.join(h14).map(lambda x: (x[0],combine(x[1][0],x[1][1])))

	return hTotal

def isSimPair(list1,list2,div):
	for t in range(len(list1) / div):
		cnt = 0
		for i in range(div):
			if list1[i + t * 2] == list2[i + t * 2]:
				cnt += 1
				if cnt == div:
					return True
			else:
				continue
		# if cnt == div:
		# 	return True
	return False

def computJac(list1,list2):
	cnt = 0
	i = 0
	j = 0
	while (i < len(list1) and j < len(list2)):
		if list1[i] == list2[j]:
			cnt += 1
			i += 1
			j += 1
		elif list1[i] > list2[j]:
			j += 1
		else:
			i += 1
	jacSim = 0
	
	jacSim = float(cnt) / float(len(list2) + len(list1) - cnt)
	
	return jacSim

def findJacSim(data,candidate):
	matrix = data.map(lambda x: (int(x[1]),int(x[0]))).groupByKey().sortByKey(True).collect()
	dic = {}
	for m in matrix:
		dic[m[0]] = list(m[1])
	# for m in range(len(matrix)):
	# 	dic[matrix[m]] = list(matrix[m][1])
	# for v in matrix:
	# 	print "%s,%s" % (v[0],list(v[1]))
	for i in range(len(candidate)):
		movie1 = candidate[i][0] 
		movie2 = candidate[i][1] 
		# print "user1: ",movie1
		# print "user2: ",movie2
		# print "matrix[user1][0]: ",matrix[movie1][0]
		# print "matrix[user2][0]: ",matrix[movie2][0]
		candidate[i] = (movie1,movie2,computJac(dic[movie1],dic[movie2]))
	return candidate


INPUT_CSV = sys.argv[1]
OUTPUT_TXT = sys.argv[2]
# SIM_PAIR_CSV = sys.argv[3]
ROW = 2
# fileToOpen.write("UserID,MovieId,Pred_rating" + "\n") 

sc = SparkContext(appName="ChiWei_Liu_hw4")
data = sc.textFile(INPUT_CSV,None,False)
dataHeader = data.first()
data = data.filter(lambda x: x!= dataHeader).map(lambda x: x.split(','))

###########

hTotal = hashMultipleTimes(data).sortByKey(True).collect()
# print "hTotal: ",hTotal 
res = []
numTotal = len(hTotal)
for mov1 in range(numTotal - 1):
	for mov2 in range(mov1 + 1, numTotal):
		if isSimPair(hTotal[mov1][1],hTotal[mov2][1],ROW):
			res.append((hTotal[mov1][0],hTotal[mov2][0]))
# print res
print "Start to compute Jaccard Similarity!!"
out = findJacSim(data,res)
candidPairs = sc.parallelize(out)
candidPairs = candidPairs.filter(lambda x: x[2] >= 0.5)
result = candidPairs.collect()
fileToOpen = open(OUTPUT_TXT, 'w')
for v in result:
	fileToOpen.write("%s,%s,%s" % (v[0],v[1],v[2]) + "\n") 
fileToOpen.close()

# pairs = candidPairs.map(lambda x: (x[0],x[1]))

# simPair = sc.textFile(SIM_PAIR_CSV,None,False).map(lambda x: x.split(','))
# simPair = simPair.map(lambda x: (int(x[0]),int(x[1])))

# tp = simPair.intersection(pairs).count()
# fp = pairs.subtract(simPair).count()
# fn = simPair.subtract(pairs).count()
# precision = float(tp) / float(tp + fp)
# recall = float(tp) / float(tp + fn)
# print "band: ",6," row: ",ROW
# print "Precision ", precision
# print "Recall: ",recall

##########

tTotalEnd = time.time()
print "It cost %f sec" % (tTotalEnd - tTotalStart)
# for v in out:
# 	print '%s,%s' % (v[0],list(v[1]))
