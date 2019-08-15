from pyspark import SparkConf, SparkContext

spark_conf = SparkConf().setMaster("local").setAppName("customer-amount")
sc = SparkContext(conf=spark_conf)

spark_file = sc.textFile("dataset/customer-orders/customer-orders.csv")

customer_sales = spark_file.map(lambda x: (x.split(',')[0], float(x.split(',')[2])))
amount_by_customer = customer_sales.reduceByKey(lambda x, y: x + y)
sorted_amount_by_customer = amount_by_customer.sortBy(lambda x: x[1])

for key, value in sorted_amount_by_customer.collect():
    print("customer ID {}: ${}".format(key, round(value, 2)))
