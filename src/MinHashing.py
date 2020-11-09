from os import listdir
from os.path import isdir, join

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, ArrayType
from pyspark.ml.feature import NGram, HashingTF, IDF, Tokenizer
from pyspark.ml import Pipeline
from pyspark.ml.linalg import SparseVector, VectorUDT
import pyspark.sql.functions as F
from pyspark.sql import Column

import random

class MinHashing:

    def __init__(self, df, spark, sc):
        self.spark = spark
        self.df = df
        self.sc = sc


#Create the characteristic matrix
    def booleanMatrix(self):
        SetShingling = set()
        Rows = self.df.count()
        datas = self.df.collect()
        for i in range(Rows):
            SetShingling = SetShingling.union(set(datas[i]["features"].indices))
        Setint = set()
        Setint = [int(i) for i in SetShingling] #Containes the set of all the shinglins for all documents

        #create dataframe with the first row containing all the shingles
        matrix = self.spark.createDataFrame(self.sc.parallelize(Setint), IntegerType(), "value") 

        
        for i in range(Rows):
            listsh = set(datas[i]["features"].indices) #Get the set of ngram for the current row
            listcontains = F.udf(lambda value: value in listsh) #contains function
            matrix = matrix.withColumn(datas[i]["docName"], listcontains(F.col("value")))#Set true if the doc contains the shingle

        return matrix


#Return the signature matrix
    def minHash(self, matrix, k):
        c = matrix.count()
        signature = []
        #Initialize the Signature matrix to a value higher than the number of hash function
        for i in range(k):
            tmp = []
            for j in range(len(matrix.columns)-1):
                tmp.append(c+1)
            signature.append(tmp)

        first = True
        a = []
        b = []

        m = matrix.collect()
        
        #Create the k hash functions
        for i in range(c):
            tmpHash = []
            for j in range(k):
                if first:
                    a.append(random.randint(1, pow(2, 32)))
                    b.append(random.randint(1, pow(2, 32)))
                tmpHash.append((a[j]*i+b[j])%c)


            if first:
                first = False
            #Compute the hash and store it in the signature matrix if the compute value is less than the actual value
            for j in range(1, len(matrix.columns)):
                if m[i][j] == "true":
                    for l in range(k):
                        if tmpHash[l] < signature[l][j-1]:
                            signature[l][j-1] = tmpHash[l]
        
        signature = self.spark.createDataFrame(signature, matrix.columns[1:]) #Datafram containing the signature function
        return signature






