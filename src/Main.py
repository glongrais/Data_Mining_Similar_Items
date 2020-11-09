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
    names = sign.columns
    datas = sign.collect()
    rowIndex = 0
    for i in datas:
        colIndex = 0
        for j in i:
            if isinstance(j, float):
                if j >= float(sys.argv[4]):
                    print(""+names[colIndex]+" and "+names[rowIndex]+" have a "+str(j)+" similarity")
            colIndex +=1
        rowIndex +=1 
                    



shin = Shingling("Datas",int(sys.argv[2]),spark) #Call the Shingling class to perform k-gram for the documents in the "Datas" folder
df = shin.getNGram()

if sys.argv[1] == "compare":
    compare()

if sys.argv[1] == "signature":
    compareSignature(int(sys.argv[3]))







