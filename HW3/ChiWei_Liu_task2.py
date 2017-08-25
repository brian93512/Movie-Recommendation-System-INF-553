import os, sys
from pyspark import SparkContext
from operator import add
import itertools
import time
import copy
import math

tTotalStart = time.time()

def findW(user1, user2, itemSet1, itemSet2,item):
	firstIdx = 0
	secondIdx = 0
	sumA = 0
	sumB = 0
	comIdA = []
	comIdB = []
	isContainItem = False
	itemRated = -1
	w = 0

	while (firstIdx < len(itemSet1) and secondIdx < len(itemSet2)):
		if itemSet1[firstIdx][0] == itemSet2[secondIdx][0]:
			sumA += itemSet1[firstIdx][1]
			sumB += itemSet2[secondIdx][1]
			comIdA.append(firstIdx)
			comIdB.append(secondIdx)
			firstIdx += 1
			secondIdx += 1
		elif itemSet1[firstIdx][0] > itemSet2[secondIdx][0]:
			if itemSet2[secondIdx][0] == item:
				isContainItem = True
				itemRated = itemSet2[secondIdx][1]
			secondIdx += 1
		elif itemSet1[firstIdx][0] < itemSet2[secondIdx][0]:
			if itemSet2[secondIdx][0] == item:
				isContainItem = True
				itemRated = itemSet2[secondIdx][1]
			firstIdx += 1
	if isContainItem == False:
		return ((user1,user2),(-1),w)

	if comIdA == [] or comIdB == []:
		return ((user1,user2),(-1),w)

	aAvg = sumA / len(comIdA)
	bAvg = sumB / len(comIdB)

	upper = 0
	lower = 0
	sumAPow = 0
	sumBPow = 0
	
	for i in range(len(comIdA)):
		idA = comIdA[i]
		idB = comIdB[i]
		partA = itemSet1[idA][1] - aAvg
		partB = itemSet2[idB][1] - bAvg
		upper += (partA) * (partB)
		sumAPow += pow(partA,2)
		sumBPow += pow(partB,2)

	# sumTotalA = 0
	# for i in range(len(itemSet1)):
	# 	sumTotalA += itemSet1[i][1]

	# totalAvg = sumTotalA / len(itemSet1)

	lower = math.sqrt(sumAPow) * math.sqrt(sumBPow)
	if not lower == 0:
		w = upper / lower
	
	ratedDiffMulWeight = 0 # ex: (r2,2 - ravg) * w1,2 
	ratedDiffMulWeight = (itemRated - bAvg) * w

	return ((user1,user2),ratedDiffMulWeight,w)


INPUT1_CSV = sys.argv[1]
INPUT2_CSV = sys.argv[2]
fileToOpen = open("ChiWei_Liu_result_task2.txt", 'w')
fileToOpen.write("UserID,MovieId,Pred_rating" + "\n") 

sc = SparkContext(appName="BrialLiu")
allData = sc.textFile(INPUT1_CSV,None,False)
dataHeader = allData.first()
allData = allData.filter(lambda x: x != dataHeader) 
tData = sc.textFile(INPUT2_CSV,None,False)
tHeader = tData.first()
tData = tData.filter(lambda x: x != tHeader)

allData.subtract(tData)

rating = allData.map(lambda x: x.split(',')).map(lambda x: (int(x[0]),(int(x[1]),float(x[2])))).groupByKey().sortByKey(True)
target = tData.map(lambda x: x.split(',')).map(lambda x: (int(x[0]),(int(x[1])))).groupByKey().sortByKey(True)
res = target.collect()
predRes = []
for line in res:
	print "user: ",line[0]
	
	targetLine = rating.filter(lambda x: x[0] == line[0])
	targetLine = targetLine.collect()
	sumTotalA = 0
	user = list(targetLine[0][1])
	for i in range(len(user)):
		sumTotalA += user[i][1]
		totalAvg = sumTotalA / len(user)
	#print totalAvg
	traingData = rating.filter(lambda x: x[0] != line[0])

	for item in line[1]:
		print "mov: ",item
		# print "user: ",line[0]
		p = 0
		
		#traingData = traingData.filter(lambda x: x)
		predWeight = traingData.filter(lambda x: x.count > 80).map(lambda x: findW(line[0],x[0],list(targetLine[0][1]),list(x[1]),item)).filter(lambda x: x[2] != 0)
		#print predWeight.collect()
		#tavg = predWeight.filter(lambda x: x[3] > 0).map(lambda x: x[3]).collect()
		#print tavg[0]
		# if predWeight.map(lambda x : list(x)).count == 0:
		# 	break
		#numUser = predWeight.count()
		upper = predWeight.map(lambda x: (1,x[1])).reduceByKey(add).collect() #.map(lambda x: x[1])
		lower = predWeight.map(lambda x: (1,abs(x[2]))).reduceByKey(add).collect()
		if upper == [] or lower == []:
			p = totalAvg
			fileToOpen.write(str(line[0]) + "," + str(item) + "," + str(p) +"\n")
			continue
		upValue = upper[0][1]
		lowValue = lower[0][1]
		#sumAvg = predWeight.map(lambda x: (1,x[3])).reduceByKey(add)
		#sumAvg = sumAvg.map(lambda x: x[1]).collect()
		#sumAvg = sumAvg[0]
		#sumAvg = sumAvg.map(lambda x: x[1]).collect()
		#sumAvg = sumAvg[0] / numUser
		if lowValue == 0:
			p = totalAvg
		else:
			p = totalAvg + upValue / lowValue
		fileToOpen.write(str(line[0]) + "," + str(item) + "," + str(p) +"\n")
		#predRes.append((line[0],item,p))
		#print predRes
		#predRes.append((line[0]))
		#predRating = weight.filter(lambda x: x[2] != 0).collect()
		#print "predRating: ",predRating
fileToOpen.close()

fhandle = sc.textFile('ChiWei_Liu_result_task2.txt',None,False)
fileHeader = fhandle.first()
result = fhandle.filter(lambda x: x != fileHeader).map(lambda x: x.split(',')).map(lambda x: ((int(x[0]),int(x[1])),float(x[2])))
allData = allData.filter(lambda x: x != dataHeader).map(lambda x: x.split(',')).map(lambda x: ((int(x[0]),int(x[1])),float(x[2]))) 
rdd = allData.join(result).map(lambda x: abs(x[1][0]-x[1][1]))
l1 = rdd.filter(lambda x: x>=0 and x<1).count()
l2 = rdd.filter(lambda x: x>=1 and x<2).count()
l3 = rdd.filter(lambda x: x>=2 and x<3).count()
l4 = rdd.filter(lambda x: x>=3 and x<4).count()
l5 = rdd.filter(lambda x: x>= 4).count()
print ">=0 and <1: ",l1
print ">=1 and <2: ",l2
print ">=2 and <3: ",l3
print ">=3 and <4: ",l4
print ">=4: ",l5


rdd1=rdd.map(lambda x:x**2).reduce(lambda x,y:x+y)
rmse=math.sqrt(rdd1/result.count())
print rmse
tTotalEnd = time.time()
print "It cost %f sec" % (tTotalEnd - tTotalStart)
# for v in out:
# 	print '%s,%s' % (v[0],list(v[1]))
