from __future__ import print_function

import sys
from bitarray import bitarray

from pyspark.sql import SparkSession


def createPairs(line):
     pairs = []
     for i in range(1, len(line)):
             pairs.append((line[i], [line[0]]))
     return pairs

def createTidset(line):
     a = bitarray(NUM_OF_ITEMS+1)
     a.setall(False)
     for tID in line[1]:
             a[int(tID)] = True
     return (line[0], a)

def calcNewBitset(line):
    (item1, item2) = line
    newItem = item1[0]+","+item2[0]
    newBitset = item1[1] | item2[1]
    return (newItem, newBitset)

def filterCartesianJoin(line):
    for item in line[0][0].split(","):
        if item < line[1][0]:
            continue
        else:
            return False
    return True

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: sparkStandAloneSolution <file> <minsupp> <maxitem>", file=sys.stderr)
        exit(-1)

    # Initialize the spark context.
    spark = SparkSession\
    .builder\
    .appName("DIM-Spark")\
    .getOrCreate()

    FILE_PATH = sys.argv[1]
    MIN_SUPP = int(sys.argv[2])
    MAX_ITEM = int(sys.argv[3])


    #dlinesRDD =  sc.textFile(FILE_PATH)
    linesRDD = spark.read.text(FILE_PATH).rdd.map(lambda r: r[0])
    #linesRDD.collect()

    NUM_OF_ITEMS =  linesRDD.count()

    itemsRDD = linesRDD.map(lambda line : line.split())
    #itemsRDD.collect()

    itemTidRDD = itemsRDD.flatMap(createPairs)
    #itemTidRDD.collect()

    itemTidsetRDD = itemTidRDD.reduceByKey(lambda a, b: a + b)
    #itemTidsetRDD.collect()

    freqItemRDD = itemTidsetRDD.filter(lambda line: len(line[1]) >= MIN_SUPP)
    #freqItemRDD.collect()

    tidsetBitsetRDD = freqItemRDD.map(createTidset)

    level = 1
    print("Level: ", level)
    print("Total no. of itemsets: ", tidsetBitsetRDD.count())
    #for (k, v) in tidsetBitsetRDD.collect():
    #    print(k)

    newCombRDD = tidsetBitsetRDD
    while(level < MAX_ITEM and newCombRDD.count() != 1):
        combRDD = newCombRDD.cartesian(tidsetBitsetRDD).filter(filterCartesianJoin)
        #combRDD.collect()
        newCombRDD = combRDD.map(calcNewBitset)
        level += 1
        print("Level: ", level)
        print("Total no. of itemsets: ", newCombRDD.count())
        #for (k, v) in newCombRDD.collect():
        #        print(k, v.count())

    spark.stop()

