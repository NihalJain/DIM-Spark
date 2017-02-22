from bitarray import bitarray

def createPairs(line):
     pairs = []
     for i in range(1, len(line)):
             pairs.append((line[i], [line[0]]))
     return pairs

def createTidset(line):
     a = bitarray(7)
     a.setall(False)
     for tID in line[1]:
             a[int(tID)] = True
     return (line[0], a)

def filterCartesianJoin(line):
    for item in line[0][0]:
        if item < line[1][0]:
            continue
        else:
            return False
    return True


def calcNewBitset(line):
    (item1, item2) = line
    newItem = item1[0]+item2[0]
    newBitset = item1[1] | item2[1]
    return (newItem, newBitset)

datasetRDD =  sc.textFile("/home/nj/pySpark/test")
datasetRDD.collect()

itemsRDD = datasetRDD.map(lambda line : line.split())
itemsRDD.collect()

itemTidRDD = itemsRDD.flatMap(createPairs)
itemTidRDD.collect()

itemTidsetRDD = itemTidRDD.reduceByKey(lambda a, b: a + b)
itemTidsetRDD.collect()

minsupp = 2
freqItemRDD = itemTidsetRDD.filter(lambda line: len(line[1]) >= minsupp)
freqItemRDD.collect()



tidsetBitsetRDD = freqItemRDD.map(createTidset)
tidsetBitsetRDD.collect()


newCombRDD = tidsetBitsetRDD
while(newCombRDD.count() != 1):
    combRDD = newCombRDD.cartesian(tidsetBitsetRDD).filter(filterCartesianJoin)
    combRDD.collect()
    newCombRDD = combRDD.map(calcNewBitset)
    newCombRDD.collect()