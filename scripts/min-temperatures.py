from pyspark import SparkConf, SparkContext

spark_conf = SparkConf().setMaster("local").setAppName("min-temperatures")
sc = SparkContext(conf=spark_conf)

spark_file = sc.textFile("dataset/1800temperatures/1800.csv")
# stationID, temperatureType and temperature in fahrenheit
temperatures = spark_file.map(lambda x: (x.split(',')[0], x.split(',')[2], float(x.split(',')[3]) * 0.1 * (9 / 5) + 32))
min_temperatures = temperatures.filter(lambda x: "TMIN" in x[1])
station_and_min_temperatures = min_temperatures.map(lambda x: (x[0], x[2]))
minimum_temperature = station_and_min_temperatures.reduceByKey(lambda x, y: min(x, y))

# the minimum
minimum = station_and_min_temperatures.min(lambda x: x[1])
print("\nThe minimum from all dataset is {}F.\n".format(round(minimum[1], 2)))

# the minimum group by station
print()
for key, value in minimum_temperature.collect():
    print("station {}: minimum of {}F".format(key, round(value, 2)))
