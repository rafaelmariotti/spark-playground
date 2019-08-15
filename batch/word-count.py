from pyspark import SparkConf, SparkContext
import re

spark_conf = SparkConf().setMaster("local").setAppName("word-count")
sc = SparkContext(conf=spark_conf)

spark_file = sc.textFile("dataset/books/book_example.txt")

# split each word as a line
# simple way:
# words = spark_file.flatMap(lambda x: x.split())
# ignoring symbols and lower/upper case:
words = spark_file.flatMap(lambda x: re.compile(r'\W+', re.UNICODE).split(x.upper()))

result = words.countByValue()

print("raw result:")

for key, value in result.items():
    readable_word = key.encode('ascii', 'ignore')
    if (readable_word):
        print("{}: {} occurrencies".format(key, value))

print("\nprinting in ascending order:\n")
word_count = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
sorted_word = word_count.sortBy(lambda x: (x[1]))

for key, value in sorted_word.collect():
    print("{}: {} occurrencies".format(key, value))
