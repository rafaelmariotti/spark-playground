from pyspark import SparkConf, SparkContext

spark_conf = SparkConf().setMaster("local").setAppName("friends-by-age")
sc = SparkContext(conf=spark_conf)

spark_file = sc.textFile("dataset/friends-100k/fake-friends-100k.data")
# getting age and total friends from each line
friends = spark_file.map(lambda x: (int(x.split(',')[2]), int(x.split(',')[3])))
#
# mapValues works only with values, i.e., x is the value
#
# reduceByKey works only with values pair, i.e.,
# given a tuple (x, y), keep key intact and apply a given function to tuple (x,y),
# where x represents the first line and y represents the second line
#
# in this example:
#
# mapValues return: [(51, (152, 1)), (19, (131, 1)), (18, (37, 1)), ... ]
# explanation:
# 51 is the age
# 152 is the number of friends
# 1 is the number of times that tuple occurred
#
# after that reduceByKey returns:
#
# [(64, (301383, 1471)), (66, (310704, 1507)), (68, (305267, 1521)), ... ]
#
# explain:
#
# 64 is the age
# 301383 is the total number of friends for age 64
# 1471 is the total number of times that age occurred
#
friends_value_as_tuple = friends.mapValues(lambda x: (x, 1))
friends_sum_and_total_occurrence = friends_value_as_tuple.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
result = friends_sum_and_total_occurrence.mapValues(lambda x: round(x[0]/x[1]))
sorted_result = result.sortByKey()

for key, value in sorted_result.collect():
    print("{} years old have an average of {} friends".format(key, value))

# the 10 most popular age are
print("\nthe most popular age are: \n")
sorted_result = result.sortBy(lambda x: x[1])
top10 = list(reversed(sorted_result.collect()))[1:10]

for key, value in top10:
    print("{} years old have an average of {} friends".format(key, value))
