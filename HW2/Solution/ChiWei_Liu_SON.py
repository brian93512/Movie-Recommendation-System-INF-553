import os, sys
from pyspark import SparkContext
from operator import add
import itertools
import time
import copy

tTotalStart = time.time()
CASE_NUM = int(sys.argv[1])
INPUT_CSV = sys.argv[2]
SUPPORT = int(sys.argv[3])

print "case number: ", CASE_NUM
print "input file: ", INPUT_CSV
print "support: ", SUPPORT

sc = SparkContext(appName="BrialLiu")
rating = sc.textFile(INPUT_CSV,None,False)
ratingHeader = rating.first()
rating = rating.filter(lambda x: x != ratingHeader) 
def findSingleFreqItem(baskets,numTotal):
	baskets = list(baskets)
	dic = {}
	freqItem = []
	numDoc = 0
	for item in baskets:
		numDoc += 1

	thresholdSupport = SUPPORT * (float(numDoc) / float(numTotal))
	print "thresholdSupport: ",thresholdSupport,"numDoc: ",numDoc

	for item in baskets:
		for i in list(item[1]):
			if dic.get(i,0) == 0:
				dic[i] = 1
			else:
				dic[i] += 1
				if (dic.get(i) > thresholdSupport and freqItem.count(i) == 0) :
					freqItem.append(int(i))

	freqItem.sort()
	yield freqItem

def createBasket(baskets):
	result = []
	for basket in baskets:
		result.append(basket[1])
	#print result
	return result


def findPairsInRdd(rdd):
	pairs = []
	for item in rdd:
		for i in range(1, len(item[1]) + 1):
			pairs.append(i)
			els = [list(x) for x in itertools.combinations(item[1],i)]
			pairs.append(els)
	yield pairs.sort

def countFreqPairs(baskets,numTotal):
	baskets = list(baskets)
	dic = {}
	allPairs = []
	freqItemPairs = []
	numDoc = 0

	for item in baskets:
		for i in range(1, len(item[1]) + 1):
			els = [list(x) for x in itertools.combinations(item[1],i)]
			for j in range(len(els)):
				els[j].sort()
				if allPairs.count(els[j]) == 0:
					allPairs.append(els[j])
		numDoc += 1

	thresholdSupport = SUPPORT * (float(numDoc) / float(numTotal))
	#print "thresholdSupport: ",thresholdSupport,"numDoc: ",numDoc

	for pair in allPairs:
		cnt = 0
		for item in baskets:
			if set(pair).issubset(set(item[1])):
				cnt += 1
				if cnt > thresholdSupport:
					freqItemPairs.append(pair)
					break
	yield freqItemPairs

def myFun(rdd):
	i = 0
	for item in rdd:
		i += 1

	yield i

def compareFreq(itemSet):
	res = []
	for key,value in itemSet:
		#print "value: ", value
		if value >= SUPPORT:
			res.append(key)
	return res

def addResultInPartition(rdd):
	sumResult = []
	for item in rdd:
		for j in range(len(item)):
			if sumResult.count(item[j]) == 0:
				sumResult.append(item[j])
	sumResult.sort()
	return sumResult

def findFreqInCandidate(basket,candidate):
	realFreqItems = []
	#print "candidate: ", candidate
	for i in range(len(candidate)):
		cntAppear = 0
		#print candidate[i]
		for k in range(len(basket)):
			for j in basket[k]:
				if candidate[i] == j:
					cntAppear += 1
				#print "candidate[i] == j"
				if cntAppear >= SUPPORT and realFreqItems.count(candidate[i]) == 0:
					realFreqItems.append(candidate[i])
					break
	return realFreqItems

def findFreqCandidate(basket,candidate):
	realFreqItems = set()
	
	for pair in candidate:
		cnt = 0
		for line in basket:
			copyPair = copy.copy(pair)
			for item in pair:
				if item in line:
					#print "copyPair: ",copyPair
					#print "remove.item: ",item
					
					copyPair.remove(item)
					#print "copyPair: ",copyPair,"cnt: ",cnt
				if not copyPair:
					cnt += 1
			if cnt >= SUPPORT:
				realFreqItems.add(tuple(sorted(pair)))

	#print "dicFreqItem: ",dicFreqItem	
	print "realFreqItems: ",realFreqItems	
	return sorted(realFreqItems)

def findFreqCandidate1(baskets,candidate):
	baskets = list(baskets)
	dic = {}
	res = []
	print list(basket[1])
	yield res

def findFreqCandidate2(baskets,candidate):
	baskets = list(baskets)
	dic = {}
	res = []
	#print "hi\n"
	return res

def transTupleToList(tup):
	resultList = []
	for i in range(len(tup)):
		for j in range(len(tup[i])):
			resultList.append(tup[i][j])
	
	return resultList

def generatePairsFromSingle(itemSet):
	
	tmp = [list(x) for x in itertools.combinations(itemSet,2)]
	tmp.sort()
	return tmp

def generateCombination(itemSet):
	#print "********** start generateCombination ***********"
	result = []
	for i in xrange(len(itemSet) - 1):
		for j in xrange(i + 1,len(itemSet)):
			#print "generateCombination: itemSet: ",itemSet
			newPair = set()
			for pair1, pair2 in zip(itemSet[i],itemSet[j]):
				newPair.add(pair1)
				newPair.add(pair2)
			if len(newPair) == len(itemSet[i]) + 1:
				result.append(sorted(newPair))
	#print "generateCombination result: ",sorted(result)
	#result = list(result)
	return sorted(result)

def generateCombination1(itemSet):
	itemSet = list(itemSet)
	result = []
	for i in xrange(len(itemSet) - 1):
		for j in xrange(i + 1,len(itemSet)):
			newPair = set()
			for pair1, pair2 in zip(itemSet[i],itemSet[j]):
				newPair.add(pair1)
				newPair.add(pair2)
				#print "newPair: ",newPair
			if len(newPair) == len(itemSet[i]) + 1:
				result.append(newPair)
	#print "sorted(result): ",
	return sorted(result)

def countCombFreq(basket,itemSet,numTotal):
	basket = list(basket)
	result = []
	thresholdSupport = SUPPORT * (float(len(basket)) / float(numTotal))
	#print "thresholdSupport: ",thresholdSupport
	cnt = 0
	for item in itemSet:
		#print "countCombFreq: item: ",item
		for line in basket:
			candidate = set(item)
			for i in item:
				if i in line:
					candidate.remove(i)
			if not candidate:
				cnt += 1
		if cnt >= thresholdSupport:
			result.append(tuple(item))
	#print "result: ",result
	yield result

def countFreqCandidate(basket,candidate):
	basket = list(basket)
	result = []
	for item in candidate:
		cnt = 0
		for line in basket:
			#print "countFreqCandidate: line: ",line
			tmp = set(item)
			for i in item:
				if i in line:
					tmp.remove(i)
			if not tmp:
				cnt += 1
		result.append((item,cnt))
	#print "tuple(candidate,cnt): ",result
	yield result


#def funSupport(rdd,num):
	#yield num
if CASE_NUM == 1:
	baskets = rating.map(lambda x: x.split(',')).map(lambda x: (int(x[0]),int(x[1]))).groupByKey()#.sortByKey(True)
	fileToOpen = open("ChiWei_Liu_SON_MovieLens.case1.txt", 'w') 
else:
	baskets = rating.map(lambda x: x.split(',')).map(lambda x: (int(x[1]),int(x[0]))).groupByKey()#.sortByKey(True)
	fileToOpen = open("ChiWei_Liu_SON_MovieLens.case2.txt", 'w') 


print "getNumPartitions: ",baskets.getNumPartitions()
basketItem = baskets.collect()
baskets = baskets.map(lambda x: set(x[1]))
lenBaskets = len(basketItem)
singleFreqItem = baskets.flatMap(lambda x: x).map(lambda x: (x,1)).reduceByKey(add)#.sortByKey(True)


singleFreqItem = singleFreqItem.mapPartitions(lambda x: compareFreq(x))
freqItemList = singleFreqItem.flatMap(lambda x: [(x,)])#.collect()
freqItemList = freqItemList.collect()
freqItemList.sort()

freqLength = len(freqItemList) - 1
for i in range(freqLength):
	fileToOpen.write("(" + str(freqItemList[i][0]) + ")" + ", ")
fileToOpen.write("(" + str(freqItemList[len(freqItemList) - 1][0]) + ")" + "\n\n")

#print "freqItemList: ",freqItemList, "len(freqItemList): ",len(freqItemList)
combPair = generateCombination(freqItemList)
freqPairCandidate = baskets.mapPartitions(lambda x: countCombFreq(x,combPair,lenBaskets))
freqPairCandidate = freqPairCandidate.flatMap(lambda x: x).distinct().collect()

tSingleEnd = time.time()
print "Create single pair candidate cost %f sec" % (tSingleEnd - tTotalStart)

cnt = 0

while  freqPairCandidate != []:
	tWhileStart = time.time()
	print "~~~~ loop %d ~~~~~" % cnt
	freqPair = baskets.mapPartitions(lambda x: countFreqCandidate(x,freqPairCandidate)).flatMap(lambda x: x).reduceByKey(add)
	freqPair = freqPair.filter(lambda x: x[1] >= SUPPORT).map(lambda x: x[0])#.sortByKey(True)
	freqPairList = freqPair.collect()
	if freqPairList == []:
		break

	freqPairList.sort()
	tCountFreqCandidate = time.time()
	print "Count frequent candidate cost %f sec" % (tCountFreqCandidate - tWhileStart)

	print "freqPairList: ",freqPairList
	#fileToOpen.write(freqPairOutPut)

	freqLength = len(freqPairList) - 1
	for i in range(freqLength):
		fileToOpen.write(str(freqPairList[i]) + ", ")
	fileToOpen.write(str(freqPairList[freqLength]) + "\n\n")

	combPair = generateCombination(freqPairList)

	tGrneratePair = time.time()
	print "Generate combination cost %f sec" % (tGrneratePair - tCountFreqCandidate)

	if combPair == []:
		break

	freqPairCandidate = baskets.filter(lambda x: len(x) >= len(combPair[0])).mapPartitions(lambda x: countCombFreq(x,combPair,lenBaskets))
	freqPairCandidate = freqPairCandidate.flatMap(lambda x: x).distinct().collect()
	cnt += 1
	tCreateCandidate = time.time()
	print "Create frequent pair candidate cost %f sec" % (tCreateCandidate - tGrneratePair)

print "freqPairList: ",freqPairList
tTotalEnd = time.time()
print "It cost %f sec" % (tTotalEnd - tTotalStart)



