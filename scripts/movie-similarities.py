from pyspark import SparkConf, SparkContext
from math import sqrt
import sys


def load_movie_names():
    with open("dataset/ml-100k/u.item", encoding="ISO-8859-1") as file:
        movie_name = {}
        for line in file:
            fields = line.split("|")
            movie_name[int(fields[0])] = fields[1]
    return movie_name


# function to filter duplicates: returns only if the movie ID is lower than the second
# this way, when duplicate items like (1, 2) is equal to (2, 1), only (1, 2) is considered
def filter_duplicates(ids_and_ratings):
    ratings = ids_and_ratings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return movie1 < movie2


def create_pair(movie_and_ratings_pair):
    movies_and_ratings = movie_and_ratings_pair[1]
    (movie1, rating1) = movies_and_ratings[0]
    (movie2, rating2) = movies_and_ratings[1]

    return ((movie1, movie2), (rating1, rating2))


def calculate_cosine_similarity(rating_pair):
    number_of_occurrences = 0
    sum_xx = sum_yy = sum_xy = 0
    for ratingX, ratingY in rating_pair:
        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        number_of_occurrences += 1

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    score = 0
    if (denominator):
        score = (numerator / (float(denominator)))

    return (score, number_of_occurrences)

# code starts here
movie_name = load_movie_names()

spark_conf = SparkConf().setMaster("local[*]").setAppName("movie-similarities")
sc = SparkContext(conf=spark_conf)

spark_file = sc.textFile("dataset/ml-100k/u.data")

# create rdd mapping user ID with his movie rating info, like (id_123, (movie_456, rating_5))
ids_and_ratings = spark_file.map(lambda x: (int(x.split()[0]), (int(x.split()[1]), float(x.split()[2]))))

# self join to create every combination
ratings_combined = ids_and_ratings.join(ids_and_ratings)

# eliminate duplicate from previous permutation
ratings_combined_without_duplicate = ratings_combined.filter(filter_duplicates)

# at this point, we have something like
# ( (userID, ((movieID, rating), (movieID, rating))), ...)
# now, we ignore the userID and work with the movie pair, creating something like
# ( ((movieID, movieID), (rating, rating)), ...)
movie_pairs = ratings_combined_without_duplicate.map(create_pair)

# group all ratings from same (movieID, movieID) tuple, generating something like
# ((movieID, movieID), ((rating, rating), (rating, rating), (rating, rating), (rating, rating), ...))
group_movie_pair_ratings = movie_pairs.groupByKey()

# calculate similarities between two movies
# preserve the key and returning a calculated value with (score, number_of_occurrences)
movie_pair_similarities_degree = group_movie_pair_ratings.mapValues(calculate_cosine_similarity).cache()

# check if an ID was given as an argument
if (len(sys.argv) > 1):

    score_threshold = 0.97
    occurences_threshold = 50

    movie_id = int(sys.argv[1])

    # filter for the specified movie, with scores and occurences higher than defined earlier
    # lambda x where x is something like ((movie_id1, movie_id2), (cosine_sim_score, occurences))
    filtered_result = movie_pair_similarities_degree.filter(lambda x: (x[0][0] == movie_id or x[0][1] == movie_id) and
                                                            x[1][0] > score_threshold and
                                                            x[1][1] >= occurences_threshold)

    # sort by quality score.
    # results = filteredResults.map(lambda pairSim: (pairSim[1], pairSim[0])).sortByKey(ascending = False).take(10)
    result = filtered_result.sortBy(lambda x: x[1], ascending=False).take(10)

    print("Top 10 similar movies for " + movie_name[movie_id] + "\n")
    for movies_sim in result:
        (movie_pair, score_similarities) = movies_sim
        # Display the similarity result that isn't the movie we're looking at
        similar_movie_id = movie_pair[0]
        if (similar_movie_id == movie_id):
            similar_movie_id = movie_pair[1]
        print("movie: " + movie_name[similar_movie_id] +
              "\tscore: " + str(score_similarities[0]) +
              "\tstrength: " + str(score_similarities[1]))
