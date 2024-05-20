# Setup
from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
spark = SparkSession.builder.appName("LeastPopularSuperhero").getOrCreate()

# Read files
lines = spark.read.text("Marvel-graph.txt")

schema = StructType([ \
    StructField("id",IntegerType(),True), \
        StructField("name",StringType(),True)])

names = spark.read.schema(schema).option("sep"," ").csv("Marvel-names.txt")

# Filter the connections to find rows with only one connection
connections = lines.withColumn("id", func.split(func.trim(func.col("value"))," ")[0]) \
    .withColumn("connections",func.size(func.split(func.trim(func.col("value"))," "))-1) \
    .groupBy(func.col("id")).agg(func.sum(func.col("connections")).alias("connections"))


minConnectionsCount = connections.agg(func.min("connections")).first()[0]
leastPopular = connections.filter(func.col("connections")==minConnectionsCount)
leastPopularNames = leastPopular.join(names,on="id")
leastPopularNames.show()
