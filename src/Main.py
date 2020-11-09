from Shingling import Shingling
from CompareSets import CompareSets
from MinHashing import MinHashing
from CompareSignatures import CompareSignatures
import sys
import numpy as np
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import Row
from pyspark.ml.feature import NGram, HashingTF, IDF, Tokenizer
sc = SparkContext('local[*]')
spark = SparkSession(sc)

def compare():
    CompareSets(df, spark)

def compareSignature(v):
    min = MinHashing(df, spark, sc)
    boolMatrix = matrix = min.booleanMatrix()
    sigMatrix = min.minHash(matrix, v)
    compSign = CompareSignatures(sigMatrix, spark, sc)
    sign = compSign.compare()
    datas = sign.collect()
    for i in datas:
        for j in i:
            if isinstance(j, float):
                if j >= 0.2:
                    print("%s and %s have a %f similarity", "te", "te", j)
                    



shin = Shingling("Datas",int(sys.argv[2]),spark)
df = shin.getNGram()

if sys.argv[1] == "compare":
    compare()

if sys.argv[1] == "signature":
    compareSignature(int(sys.argv[3]))







