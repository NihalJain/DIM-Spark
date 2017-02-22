>>> from bitarray import bitarray

>>> def createPairs(line):
...      pairs = []
...      for i in range(1, len(line)):
...              pairs.append((line[i], [line[0]]))
...      return pairs
... 
>>> def createTidset(line):
...      a = bitarray(7)
...      a.setall(False)
...      for tID in line[1]:
...              a[int(tID)] = True
...      return (line[0], a)
... 
>>> def filterCartesianJoin(line):
...     for item in line[0][0]:
...         if item < line[1][0]:
...             continue
...         else:
...             return False
...     return True
... 

>>> def calcNewBitset(line):
...     (item1, item2) = line
...     newItem = item1[0]+item2[0]
...     newBitset = item1[1] | item2[1]
...     return (newItem, newBitset)
... 
>>> datasetRDD =  sc.textFile("/home/nj/pySpark/test")
>>> datasetRDD.collect()
['1\tA   C   D', '2\tB   C', '3\tA   B   C   D', '4\tA   D   E', '5\tE']

>>> itemsRDD = datasetRDD.map(lambda line : line.split())
>>> itemsRDD.collect()
[['1', 'A', 'C', 'D'], ['2', 'B', 'C'], ['3', 'A', 'B', 'C', 'D'], ['4', 'A', 'D', 'E'], ['5', 'E']]

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

>>> tidsetBitsetRDD = freqItemRDD.map(createTidset)

>>> level = 1
>>> print("Level: ", level)
Level:  1

>>> tidsetBitsetRDD.collect()
[('C', bitarray('0111000')), ('A', bitarray('0101100')), ('D', bitarray('0101100')), ('B', bitarray('0011000')), ('E', bitarray('0000110'))]

>>> newCombRDD = tidsetBitsetRDD
>>> while(newCombRDD.count() != 1):
...     combRDD = newCombRDD.cartesian(tidsetBitsetRDD).filter(filterCartesianJoin)
...     #combRDD.collect()
...     level += 1
...     print("Level: ", level)
...     newCombRDD = combRDD.map(calcNewBitset)
...     newCombRDD.collect()
... 
Level:  2
[('CD', bitarray('0111100')), ('CE', bitarray('0111110')), ('AC', bitarray('0111100')), ('BC', bitarray('0111000')), ('AD', bitarray('0101100')), ('AB', bitarray('0111100')), ('AE', bitarray('0101110')), ('BD', bitarray('0111100')), ('DE', bitarray('0101110')), ('BE', bitarray('0011110'))]
Level:  3
[('CDE', bitarray('0111110')), ('ACD', bitarray('0111100')), ('ACE', bitarray('0111110')), ('BCD', bitarray('0111100')), ('BCE', bitarray('0111110')), ('ABC', bitarray('0111100')), ('ADE', bitarray('0101110')), ('ABD', bitarray('0111100')), ('ABE', bitarray('0111110')), ('BDE', bitarray('0111110'))]
Level:  4                                                                       
[('ACDE', bitarray('0111110')), ('BCDE', bitarray('0111110')), ('ABCD', bitarray('0111100')), ('ABCE', bitarray('0111110')), ('ABDE', bitarray('0111110'))]
Level:  5                                                                       
[('ABCDE', bitarray('0111110'))]                                                
>>>                                                                             
