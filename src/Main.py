from Shingling import Shingling
from CompareSets import CompareSets
from MinHashing import MinHashing
from CompareSignatures import CompareSignatures
from LSH import LSH
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
    boolmatrix = min.booleanMatrix() #Matrix characteristic 
    sigMatrix = min.minHash(boolmatrix, v) # Signature Matrix
    compSign = CompareSignatures(sigMatrix, spark, sc) # Compare signature to approximate jaccard similarity with signature matrix
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

    lsh = LSH(spark, sc)
    lsh.lsh(sigMatrix)
                    



shin = Shingling("Datas",int(sys.argv[2]),spark) #Create a Shingling object with parameter n for n-gram
df = shin.getNGram() #Perform Ngram on shingling object

if sys.argv[1] == "compare": #if we call compare, perform compare function for jaccard similarity 
    compare()

if sys.argv[1] == "signature": #if we call signature, perform minhashing and compare minhashing
    compareSignature(int(sys.argv[3]))





