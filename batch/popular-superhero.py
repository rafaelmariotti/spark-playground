from pyspark import SparkConf, SparkContext


def load_heroes_name():
    hero_name = {}
    with open("dataset/marvel-heroes/marvel-names.txt", encoding="ISO-8859-1") as file:
        for each_line in file:
            hero_name[int(each_line.split('"')[0])] = each_line.split('"')[1]
    return hero_name


spark_conf = SparkConf().setMaster("local").setAppName("popular-superhero")
sc = SparkContext(conf=spark_conf)

#

hero_name = sc.broadcast(load_heroes_name())

spark_file = sc.textFile("dataset/marvel-heroes/marvel-graph.txt")
heroes_with_friends = spark_file.map(lambda x: (int(x.split()[0]), len(x.split())-1))
heroes_with_friends_aggregated = heroes_with_friends.reduceByKey(lambda x, y: x + y)
max_friends_superhero = heroes_with_friends_aggregated.max(lambda x: x[1])
most_popular_superhero_name = hero_name.value[max_friends_superhero[0]]

print("The most popular superhero is {} with {} friends".format(most_popular_superhero_name, max_friends_superhero[1]))

# or

spark_name_file = sc.textFile("dataset/marvel-heroes/marvel-names.txt")
heroes_name = spark_name_file.map(lambda x: (int(x.split('"')[0]), x.split('"')[1]))

min_friends_superhero = heroes_with_friends_aggregated.min(lambda x: x[1])

most_unpopular_superhero_name = heroes_name.lookup(min_friends_superhero[0])[0]

print("Again, the most popular super hero is {}".format(most_unpopular_superhero_name))
