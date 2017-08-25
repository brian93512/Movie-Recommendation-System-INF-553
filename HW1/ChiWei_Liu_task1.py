import os, sys, csv
from pyspark import SparkContext
from operator import add

BASE_DIR = os.path.dirname(__file__)
sourceFold = "big_data"
OutputFile = 'ChiWei_Liu_result_task1_big.csv'

sc = SparkContext(appName="BrialLiu")
rating = sc.textFile("%s" % BASE_DIR + "/" + "%s" % sourceFold + "/" + "ratings.csv")

ratingHeader = rating.first()
rating = rating.filter(lambda x: x != ratingHeader) # elminate the header in the csv file


rddRating = rating.map(lambda x: x.split(',')).map(lambda x: (x[1],float(x[2])))
rddSumOfRating = rddRating.aggregateByKey((0,0),lambda U,v:(U[0] + v, U[1] + 1), lambda U1,U2: (U1[0] + U2[0], U1[1] + U2[1]))
rddAvgOfRating = rddSumOfRating.map(lambda x: (int(x[0]),x[1][0] / x[1][1])).sortByKey(True)
output = rddAvgOfRating.collect()

fileToOpen = open('%s' % BASE_DIR + '/result/' + OutputFile, 'w')

csvCursor = csv.writer(fileToOpen)
header = ['movieId','ratings']
csvCursor.writerow(header)

#print output
for v in output:
    data = ['%s' % (v[0]), '%s' % (v[1])]
    csvCursor.writerow(data)

fileToOpen.close()

