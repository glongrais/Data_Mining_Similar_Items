from Shingling import Shingling
from CompareSets import CompareSets
from MinHashing import MinHashing
from CompareSignatures import CompareSignatures

import numpy as np
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import Row
from pyspark.ml.feature import NGram, HashingTF, IDF, Tokenizer
sc = SparkContext('local[*]')
spark = SparkSession(sc)

shin = Shingling("Datas",8,spark)
df = shin.getNGram()

#comp = CompareSets(df, spark)

min = MinHashing(df, spark, sc)
boolMatrix = matrix = min.booleanMatrix()
sigMatrix = min.minHash(matrix, 50)

#sigJaccard = CompareSignatures(sigMatrix)
CompareSignatures(sigMatrix, spark, sc)