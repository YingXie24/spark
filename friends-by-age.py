from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

# define function for map later
def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile("file:///SparkCourse/fakefriends.csv")

# use map to transform file into key-value RDD (age, numFriends)
rdd = lines.map(parseLine)

# use mapValues to create tuple in each row. use reduceByKey to sum all rows, grouped by age
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
halfway = totalsByAge.collect()
print(halfway)

averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])

results = averagesByAge.collect()
sorted_results = sorted(results, key=lambda x: x[0])
for result in sorted_results:
    print(result)
