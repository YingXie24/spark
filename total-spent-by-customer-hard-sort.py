from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerOrders")
sc = SparkContext(conf = conf)

# define function for map later
def parseLine(line):
    fields = line.split(',')
    customer_id = int(fields[0])
    transaction_amount = float(fields[2])
    return (customer_id, transaction_amount)


lines = sc.textFile("C:\SparkCourse\customer-orders.csv")

# map each line to key/value pairs of customer ID and dollar amount
mappedInput = lines.map(parseLine)

# use reduceByKey to add up amount spent by customer ID
customer_spending = mappedInput.reduceByKey(lambda x,y: x+y)

# swap key value
customer_spending_sorted = customer_spending.map(lambda x: (x[1], x[0])).sortByKey()

# collect() the results and print them
result = customer_spending_sorted.collect()

for item in result:
    print(f"{item[1]}\t{item[0]:.2f}")