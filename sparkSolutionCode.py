datasetRDD =  sc.textFile("/home/nj/pySpark/test")
datasetRDD.collect()

itemsRDD = datasetRDD.map(lambda line : line.split())
itemsRDD.collect()

def createPairs(line):
     pairs = []
     for i in range(1, len(line)):
             pairs.append((line[i], [line[0]]))
     return pairs


itemTidRDD = itemsRDD.flatMap(createPairs)
itemTidRDD.collect()

itemTidsetRDD = itemTidRDD.reduceByKey(lambda a, b: a + b)
itemTidsetRDD.collect()

minsupp = 2
freqItemRDD = itemTidsetRDD.filter(lambda line: len(line[1]) >= minsupp)
freqItemRDD.collect()

from bitarray import bitarray
def createTidset(line):
     a = bitarray(7)
     a.setall(False)
     for tID in line[1]:
             a[int(tID)] = True
     return (line[0], a)


tidsetBitsetRDD = freqItemRDD.map(createTidset)
tidsetBitsetRDD.collect()

#itemCount = tidsetBitsetRDD.map(lambda line: (line[0], line[1].count()))
#itemCountRDD = tidsetBitsetRDD.map(lambda line: (line[0], line[1].count()))
#itemCountRDD.collect()

#combRDD = freqItemRDD.cartesian(freqItemRDD).filter(lambda a, b: a[0] < b[0])
comb2RDD = tidsetBitsetRDD.cartesian(tidsetBitsetRDD).filter(lambda line: line[0][0] < line[1][0])
comb2RDD.collect()

def calcNewBitset(line):
	(item1, item2) = line
	newItem = item1[0]+item2[0]
    newBitset = item1[1] | item2[1]
    return (newItem, newBitset)

fialComb2RDD = comb2RDD.map(calcNewBitset)
finalComb2RDD.collect()

comb3RDD = finalComb2RDD.cartesian(tidsetBitsetRDD).filter(lambda line: line[0][0][0] < line[1][0][0] and line[0][0][1] < line[1][0][0])
comb3RDD.collect()

finalComb3RDD = comb3RDD.map(calcNewBitset)
finalComb3RDD.collect()

#flatComb3RDD = finalComb3RDD.map(lambda line: (line[0][0], line[0][1], line[1]))
#flatComb3RDD.collect()

comb4RDD = finalComb3RDD.cartesian(tidsetBitsetRDD).filter(lambda line: line[0][0][0] < line[1][0][0] and line[0][0][0] < line[1][0][0] and line[0][0][2] < line[1][0][0])
comb4RDD.collect()

finalComb4RDD = comb4RDD.map(calcNewBitset)
finalComb4RDD.collect()

#flatComb4RDD = comb4RDD.map(lambda line: (line[0][0], line[0][1], line[0][2], line[1]))
#flatComb4RDD.collect()

comb5RDD = finalComb4RDD.cartesian(tidsetBitsetRDD).filter(lambda line: line[0][0][0] < line[1][0][0] and line[0][0][0] < line[1][0][0] and line[0][0][2] < line[1][0][0] and line[0][0[3] < line[1][0][0])
comb5RDD.collect()

finalComb5RDD = comb5RDD.map(calcNewBitset)
finalComb5RDD.collect()

