>>> datasetRDD =  sc.textFile("/home/nj/pySpark/test")
>>> datasetRDD.collect()
['1\tA   C   D', '2\tB   C', '3\tA   B   C   D', '4\tA   D   E', '5\tE']

>>> itemsRDD = datasetRDD.map(lambda line : line.split())
>>> itemsRDD.collect()
[['1', 'A', 'C', 'D'], ['2', 'B', 'C'], ['3', 'A', 'B', 'C', 'D'], ['4', 'A', 'D', 'E'], ['5', 'E']]

>>> def createPairs(line):
...      pairs = []
...      for i in range(1, len(line)):
...              pairs.append((line[i], [line[0]]))
...      return pairs
... 

>>> itemTidRDD = itemsRDD.flatMap(createPairs)
>>> itemTidRDD.collect()
[('A', ['1']), ('C', ['1']), ('D', ['1']), ('B', ['2']), ('C', ['2']), ('A', ['3']), ('B', ['3']), ('C', ['3']), ('D', ['3']), ('A', ['4']), ('D', ['4']), ('E', ['4']), ('E', ['5'])]

>>> itemTidsetRDD = itemTidRDD.reduceByKey(lambda a, b: a + b)
>>> itemTidsetRDD.collect()
[('C', ['1', '2', '3']), ('A', ['1', '3', '4']), ('D', ['1', '3', '4']), ('B', ['2', '3']), ('E', ['4', '5'])]

>>> minsupp = 2
>>> freqItemRDD = itemTidsetRDD.filter(lambda line: len(line[1]) >= minsupp)
>>> freqItemRDD.collect()
[('C', ['1', '2', '3']), ('A', ['1', '3', '4']), ('D', ['1', '3', '4']), ('B', ['2', '3']), ('E', ['4', '5'])]

>>> from bitarray import bitarray
>>> def createTidset(line):
...      a = bitarray(7)
...      a.setall(False)
...      for tID in line[1]:
...              a[int(tID)] = True
...      return (line[0], a)
... 

>>> tidsetBitsetRDD = freqItemRDD.map(createTidset)
>>> tidsetBitsetRDD.collect()
[('C', bitarray('0111000')), ('A', bitarray('0101100')), ('D', bitarray('0101100')), ('B', bitarray('0011000')), ('E', bitarray('0000110'))]

>>> #itemCount = tidsetBitsetRDD.map(lambda line: (line[0], line[1].count()))
... #itemCountRDD = tidsetBitsetRDD.map(lambda line: (line[0], line[1].count()))
... #itemCountRDD.collect()
... 
>>> #combRDD = freqItemRDD.cartesian(freqItemRDD).filter(lambda a, b: a[0] < b[0])
... comb2RDD = tidsetBitsetRDD.cartesian(tidsetBitsetRDD).filter(lambda line: line[0][0] < line[1][0])
>>> comb2RDD.collect()
[(('C', bitarray('0111000')), ('D', bitarray('0101100'))), (('C', bitarray('0111000')), ('E', bitarray('0000110'))), (('A', bitarray('0101100')), ('C', bitarray('0111000'))), (('B', bitarray('0011000')), ('C', bitarray('0111000'))), (('A', bitarray('0101100')), ('D', bitarray('0101100'))), (('A', bitarray('0101100')), ('B', bitarray('0011000'))), (('A', bitarray('0101100')), ('E', bitarray('0000110'))), (('B', bitarray('0011000')), ('D', bitarray('0101100'))), (('D', bitarray('0101100')), ('E', bitarray('0000110'))), (('B', bitarray('0011000')), ('E', bitarray('0000110')))]

>>> def calcNewBitset(line):
...     (item1, item2) = line
...     newItem = item1[0]+item2[0]
...     newBitset = item1[1] | item2[1]
...     return (newItem, newBitset)
... 
>>> finalComb2RDD = comb2RDD.map(calcNewBitset)
>>> finalComb2RDD.collect()
[('CD', bitarray('0111100')), ('CE', bitarray('0111110')), ('AC', bitarray('0111100')), ('BC', bitarray('0111000')), ('AD', bitarray('0101100')), ('AB', bitarray('0111100')), ('AE', bitarray('0101110')), ('BD', bitarray('0111100')), ('DE', bitarray('0101110')), ('BE', bitarray('0011110'))]

>>> comb3RDD = comb2RDD.cartesian(tidsetBitsetRDD).filter(lambda line: line[0][0][0] < line[1][0][0] and line[0][1][0] < line[1][0][0])
>>> comb3RDD.collect()
[((('C', bitarray('0111000')), ('D', bitarray('0101100'))), ('E', bitarray('0000110'))), ((('A', bitarray('0101100')), ('C', bitarray('0111000'))), ('D', bitarray('0101100'))), ((('A', bitarray('0101100')), ('C', bitarray('0111000'))), ('E', bitarray('0000110'))), ((('B', bitarray('0011000')), ('C', bitarray('0111000'))), ('D', bitarray('0101100'))), ((('B', bitarray('0011000')), ('C', bitarray('0111000'))), ('E', bitarray('0000110'))), ((('A', bitarray('0101100')), ('B', bitarray('0011000'))), ('C', bitarray('0111000'))), ((('A', bitarray('0101100')), ('D', bitarray('0101100'))), ('E', bitarray('0000110'))), ((('A', bitarray('0101100')), ('B', bitarray('0011000'))), ('D', bitarray('0101100'))), ((('A', bitarray('0101100')), ('B', bitarray('0011000'))), ('E', bitarray('0000110'))), ((('B', bitarray('0011000')), ('D', bitarray('0101100'))), ('E', bitarray('0000110')))]

>>> flatComb3RDD = comb3RDD.map(lambda line: (line[0][0], line[0][1], line[1]))
>>> flatComb3RDD.collect()
[(('C', bitarray('0111000')), ('D', bitarray('0101100')), ('E', bitarray('0000110'))), (('A', bitarray('0101100')), ('C', bitarray('0111000')), ('D', bitarray('0101100'))), (('A', bitarray('0101100')), ('C', bitarray('0111000')), ('E', bitarray('0000110'))), (('B', bitarray('0011000')), ('C', bitarray('0111000')), ('D', bitarray('0101100'))), (('B', bitarray('0011000')), ('C', bitarray('0111000')), ('E', bitarray('0000110'))), (('A', bitarray('0101100')), ('B', bitarray('0011000')), ('C', bitarray('0111000'))), (('A', bitarray('0101100')), ('D', bitarray('0101100')), ('E', bitarray('0000110'))), (('A', bitarray('0101100')), ('B', bitarray('0011000')), ('D', bitarray('0101100'))), (('A', bitarray('0101100')), ('B', bitarray('0011000')), ('E', bitarray('0000110'))), (('B', bitarray('0011000')), ('D', bitarray('0101100')), ('E', bitarray('0000110')))]

>>> comb4RDD = flatComb3RDD.cartesian(tidsetBitsetRDD).filter(lambda line: line[0][0][0] < line[1][0][0] and line[0][1][0] < line[1][0][0] and line[0][2][0] < line[1][0][0])
>>> comb4RDD.collect()
[((('A', bitarray('0101100')), ('C', bitarray('0111000')), ('D', bitarray('0101100'))), ('E', bitarray('0000110'))), ((('B', bitarray('0011000')), ('C', bitarray('0111000')), ('D', bitarray('0101100'))), ('E', bitarray('0000110'))), ((('A', bitarray('0101100')), ('B', bitarray('0011000')), ('C', bitarray('0111000'))), ('D', bitarray('0101100'))), ((('A', bitarray('0101100')), ('B', bitarray('0011000')), ('C', bitarray('0111000'))), ('E', bitarray('0000110'))), ((('A', bitarray('0101100')), ('B', bitarray('0011000')), ('D', bitarray('0101100'))), ('E', bitarray('0000110')))]

>>> flatComb4RDD = comb4RDD.map(lambda line: (line[0][0], line[0][1], line[0][2], line[1]))
>>> flatComb4RDD.collect()
[(('A', bitarray('0101100')), ('C', bitarray('0111000')), ('D', bitarray('0101100')), ('E', bitarray('0000110'))), (('B', bitarray('0011000')), ('C', bitarray('0111000')), ('D', bitarray('0101100')), ('E', bitarray('0000110'))), (('A', bitarray('0101100')), ('B', bitarray('0011000')), ('C', bitarray('0111000')), ('D', bitarray('0101100'))), (('A', bitarray('0101100')), ('B', bitarray('0011000')), ('C', bitarray('0111000')), ('E', bitarra