from pyspark.sql import SparkSession

# Create spark session
spark= SparkSession.builder.appName("SparkSQL").getOrCreate()

# import csv with header
people = spark.read.option("header","true").option("inferSchema","true").csv("fakefriends-header.csv")

# create table from dataframe. This is so that we can write SQL commands
people.createOrReplaceTempView("people_table")

# query
query = spark.sql("SELECT age, ROUND(AVG(friends),0) AS friends_avg from people_table GROUP BY age ORDER BY age")

# display results
query.show()

spark.stop()
