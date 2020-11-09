from Shingling import Shingling
from CompareSets import CompareSets
from MinHashing import MinHashing
from CompareSignatures import CompareSignatures
from LSH import LSH
import sys
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local[*]')
spark = SparkSession(sc)

def compare():
    CompareSets(df, spark)

def compareSignature(k):
    min = MinHashing(df, spark, sc)
    boolmatrix = min.booleanMatrix() #Matrix characteristic 
    sigMatrix = min.minHash(boolmatrix, k) # Signature Matrix
    compSign = CompareSignatures( spark, sc) # Compare signature to approximate jaccard similarity with signature matrix
    sign = compSign.compare(sigMatrix)
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

def Lsh(k,b):
    min = MinHashing(df, spark, sc)
    boolmatrix = min.booleanMatrix() #Matrix characteristic 
    sigMatrix = min.minHash(boolmatrix, k) # Signature Matrix
    compSign = CompareSignatures( spark, sc) # Compare signature to approximate jaccard similarity with signature matrix
    sign = compSign.compare(sigMatrix)
    #If we want to compare LSH against minHashing
    # names = sign.columns
    # datas = sign.collect()
    # rowIndex = 0
    # for i in datas:
    #     colIndex = 0
    #     for j in i:
    #         if j >= float(sys.argv[4]):
    #             print(""+names[colIndex]+" and "+names[rowIndex]+" have a "+str(j)+" similarity")
    #         colIndex +=1
    #     rowIndex +=1 

    lsh = LSH(spark, sc)
    docSimilar = lsh.lsh(sigMatrix,b)

    print()

    # Compare signature vector of all potential similar items
    for docs in docSimilar:
        sign = compSign.compare(sigMatrix.select(docs[0], docs[1]))
        datas = sign.collect()
        names = sign.columns
        if datas[0][1] >= float(sys.argv[4]):
            print(""+names[0]+" and "+names[1]+" have a "+str(datas[0][1])+" similarity")



                    



shin = Shingling("Datas",int(sys.argv[2]),spark) #Create a Shingling object with parameter n for n-gram
df = shin.getNGram() #Perform Ngram on shingling object

if sys.argv[1] == "jaccard": #if we call compare, perform compare function for jaccard similarity 
    compare()

if sys.argv[1] == "minHashing": #if we call signature, perform minhashing and compare minhashing
    compareSignature(int(sys.argv[3]))

if sys.argv[1] == "LSH": 
    Lsh(int(sys.argv[3]), int(sys.argv[5]))





