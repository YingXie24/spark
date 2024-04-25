from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("sparkSQL").getOrCreate()


#load the data as a dataframe with a schema
schema = StructType([\
StructField("customer_id",IntegerType(), True),\
StructField("item_id", IntegerType(), True),\
StructField("transaction_amount", FloatType(), True)])

df=spark.read.schema(schema).csv("customer-orders.csv")

# group by cust_id
customer_spend = df.groupBy("customer_id").sum("transaction_amount")

# round transaction_amount to 2 decimal places
df1=customer_spend.withColumn("total_spend",func.round(func.col("sum(transaction_amount)"),2))

# show the results sorted
df1.select("customer_id","total_spend").sort("total_spend").show(df1.count())

spark.stop()

