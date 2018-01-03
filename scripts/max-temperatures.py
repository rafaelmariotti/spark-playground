from pyspark import SparkConf, SparkContext

spark_conf = SparkConf().setMaster("local").setAppName("max-temperatures")
sc = SparkContext(conf=spark_conf)

spark_file = sc.textFile("dataset/1800temperatures/1800.csv")
# stationID, temperatureType and temperature in fahrenheit
temperatures = spark_file.map(lambda x: (x.split(',')[0], x.split(',')[2], float(x.split(',')[3]) * 0.1 * (9 / 5) + 32))
max_temperatures = temperatures.filter(lambda x: "TMAX" in x[1])
station_and_max_temperatures = max_temperatures.map(lambda x: (x[0], x[2]))
maximum_temperature = station_and_max_temperatures.reduceByKey(lambda x, y: max(x, y))

# the maximum
maximum = station_and_max_temperatures.max(lambda x: x[1])
print("\nThe maximum from all dataset is {}F.\n".format(round(maximum[1], 2)))

# the maximum group by station
print()
for key, value in maximum_temperature.collect():
    print("station {}: maximum of {}F".format(key, round(value, 2)))
