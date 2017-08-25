import os, sys, csv
from pyspark import SparkContext
from operator import add

BASE_DIR = os.path.dirname(__file__)
sourceFold = "small_data"
OutputFile = 'ChiWei_Liu_result_task2_small.csv'

sc = SparkContext(appName="BrialLiu")
rating = sc.textFile("%s" % BASE_DIR + "/" + "%s" % sourceFold + "/" + "ratings.csv")
tag = sc.textFile("%s" % BASE_DIR + "/" + "%s" % sourceFold + "/" + "tags.csv", None, False) # set the file as utf-8, take off the unicode

ratingHeader = rating.first()
tagHeader = tag.first()

rating = rating.filter(lambda x: x != ratingHeader) # elminate the header in the csv file
tag = tag.filter(lambda x: x != tagHeader)

rddRating = rating.map(lambda x: x.split(',')).map(lambda x: (x[1],float(x[2])))
rddTag = tag.map(lambda x: x.split(',')).map(lambda x: (int(x[1]),x[2])).sortByKey(True)

rddSumOfRating = rddRating.aggregateByKey((0,0),lambda U,v:(U[0] + v, U[1] + 1), lambda U1,U2: (U1[0] + U2[0], U1[1] + U2[1]))

rddAvgOfRating = rddSumOfRating.map(lambda x: (int(x[0]),x[1][0] / x[1][1])).sortByKey(True)

rddTmp = rddTag.join(rddAvgOfRating).map(lambda x: (x[1][0],x[1][1]))
rddSumOfTagRating = rddTmp.aggregateByKey((0,0),lambda U,v:(U[0] + v, U[1] + 1), lambda U1,U2: (U1[0] + U2[0], U1[1] + U2[1]))
rddAvgOfTagRating = rddSumOfTagRating.map(lambda x: (x[0],x[1][0] / x[1][1])).sortByKey(False)

output = rddAvgOfTagRating.collect()

fileToOpen = open('%s' % BASE_DIR + '/result/' + OutputFile, 'w') # open a csv file

csvCursor = csv.writer(fileToOpen)
header = ['tag','rating_avg']
csvCursor.writerow(header)

for v in output:
	data = ['%s' % (v[0]), '%s' % (v[1])]
	csvCursor.writerow(data)

fileToOpen.close()	
