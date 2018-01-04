from pyspark import SparkConf, SparkContext

spark_conf = SparkConf().setMaster("local").setAppName("popular-movie")
sc = SparkContext(conf=spark_conf)

spark_file = sc.textFile("dataset/movieLens/u.data")

movies = spark_file.map(lambda x: (x.split('\t')[1], 1))
count_movies = movies.reduceByKey(lambda x, y: x + y)
sorted_count_movies = count_movies.sortBy(lambda x: x[1])

for key, value in sorted_count_movies.collect():
    print("movie ID {}: {} times".format(key, value))
