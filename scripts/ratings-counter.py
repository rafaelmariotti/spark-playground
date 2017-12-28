from pyspark import SparkConf, SparkContext

spark_conf = SparkConf().setMaster("local").setAppName("ratings-histogram")
spark_context = SparkContext(conf=spark_conf)

spark_file = spark_context.textFile("dataset/ml-100k/u.data")
ratings = spark_file.map(lambda x: x.split()[2])
result = ratings.countByValue()

sorted_result = sorted(result.items())
for key, value in sorted_result:
    print("{}: {} ratings".format(key, value))
