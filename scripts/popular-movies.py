from pyspark import SparkConf, SparkContext


def load_movie_names():
    names = {}
    with open("dataset/movieLens/u.item", encoding="ISO-8859-1") as file:
        for each_line in file:
            names[int(each_line.split('|')[0])] = each_line.split('|')[1]
    return names


spark_conf = SparkConf().setMaster("local").setAppName("popular-movie")
sc = SparkContext(conf=spark_conf)

movie_name_dict = sc.broadcast(load_movie_names())
spark_file = sc.textFile("dataset/movieLens/u.data")

movies = spark_file.map(lambda x: (int(x.split('\t')[1]), 1))
count_movies = movies.reduceByKey(lambda x, y: x + y)
sorted_count_movies = count_movies.sortBy(lambda x: x[1])
sorted_movies_with_name = sorted_count_movies.map(lambda x: (movie_name_dict.value[x[0]], x[1]))

for key, value in sorted_movies_with_name.collect():
    print("Movie {}: rated {} times".format(key, value))
