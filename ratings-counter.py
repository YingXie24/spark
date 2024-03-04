from pyspark import SparkConf, SparkContext
import collections

# Run locally on one process
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

# Create RDD containing the dataset containing tv ratings
lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")
print(lines)

# extract ratings field into a separate RDD
ratings = lines.map(lambda x: x.split()[2])

# RDD action to count by ratings. This produces a tuple 
result = ratings.countByValue()

# Python code to print result
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
