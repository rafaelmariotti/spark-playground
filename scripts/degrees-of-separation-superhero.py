from pyspark import SparkConf, SparkContext

spark_conf = SparkConf().setMaster("local").setAppName("degrees-of-separation-superheroes")
sc = SparkContext(conf=spark_conf)

start_hero_id = 5306  # spider man
target_hero_id = 14  # ADAM (wtf?)

hit_counter = sc.accumulator(0)


def convertMarvelGraphToBFS(line):
    fields = line.split()
    hero_id = int(fields[0])

    hero_conn = []
    for conn in fields[1:]:
        hero_conn.append(int(conn))

    color = 'WHITE'
    distance = 9999999999

    if(hero_id == start_hero_id):
        color = 'GRAY'
        distance = 0
    return (hero_id, (hero_conn, distance, color))


def bfsMap(node):  # node var example: (1, ([2,3,4,5], 99999, 'WHITE'))
    hero_id = node[0]
    hero_conn = node[1][0]
    distance = node[1][1]
    color = node[1][2]

    result = []

    if (color == 'GRAY'):
        for conn in hero_conn:
            new_hero_id = conn
            new_distance = distance + 1
            new_color = 'GRAY'

            if (target_hero_id == new_hero_id):
                hit_counter.add(1)

            new_entry = (new_hero_id, ([], new_distance, new_color))
            result.append(new_entry)
        color = 'BLACK'

    result.append((hero_id, (hero_conn, distance, color)))
    return result


def bfsReduce(data1, data2):
    edge1 = data1[0]
    edge2 = data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]

    distance = 9999999999
    color = color1
    new_edge = []

    # combine all superhero friends in a single array
    if (len(edge1) > 0):
        new_edge.extend(edge1)
    if (len(edge2) > 0):
        new_edge.extend(edge2)

    # save the minimum distance to set
    if (distance1 < distance):
        distance = distance1

    if (distance2 < distance):
        distance = distance2

    # preserve darkest color
    # if one of the nodes was already processed, set as GRAY or BLACK
    if (color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK')):
        color = color2

    if (color1 == 'GRAY' and color2 == 'BLACK'):
        color = 'BLACK'

    if (color2 == 'WHITE' and (color1 == 'GRAY' or color1 == 'BLACK')):
        color = color1
    # if one of the nodes was already processed and finished
    if (color2 == 'GRAY' and color1 == 'BLACK'):
        color = 'BLACK'

    return (new_edge, distance, color)

spark_file = sc.textFile("dataset/marvel-heroes/marvel-graph.txt")
new_superheroes_graph = spark_file.map(convertMarvelGraphToBFS)

# setting 10 loops - assuming that we will never have more than 10 degrees of separation
for x in range(0, 10):
    print("Running BFS iteration number {}".format(str(x+1)))

    mapped = new_superheroes_graph.flatMap(bfsMap)

    print("Found {} characters to analyze".format(mapped.count()))

    if (hit_counter.value > 0):
        print("Superhero found with {} different graph directions".format(hit_counter.value))
        break

    new_superheroes_graph = mapped.reduceByKey(bfsReduce)
