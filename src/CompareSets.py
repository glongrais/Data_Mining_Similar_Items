from os import listdir
from os.path import isdir, join

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType
from pyspark.ml.feature import NGram, HashingTF, IDF, Tokenizer
from pyspark.ml import Pipeline
from pyspark.ml.linalg import SparseVector, VectorUDT
from pyspark.sql.functions import col
from pyspark.sql import Column


class CompareSets:
    def __init__(self, df, spark):
        self.spark = spark
        self.df = df
        self.jaccard()

#Perform jaccard similarity calculation
    def jaccard(self):

        s1 = set()
        s2 = set()

        datas = self.df.select("id","features").collect()#Collect the ngram for each documents

        schema = StructType([StructField("id", IntegerType(), True), StructField("jaccardSimilarity", VectorUDT(), True)])
        tmpDf = self.spark.createDataFrame([],schema)#Create new dataframe for the jaccardSimilarity

        #Loop for jaccard similarity
        for t in datas:
            vect = {}
            tmpT = set(t.features.indices)
            for u in datas:
                tmpU = set(u.features.indices)
                s1 = tmpT.union(tmpU)
                s2 = tmpT.intersection(tmpU)
                try:
                    vect[u.id] = len(s2)/len(s1) #Jaccard similarity calculation
                except ZeroDivisionError:
                    vect[u.id] = 0

            sparceVect = SparseVector(len(vect), vect)
            tmpDf = tmpDf.union(self.spark.createDataFrame([(t.id, sparceVect)], schema))

        self.df = self.df.join(tmpDf, on=["id"])

        self.df.select("docName", "jaccardSimilarity").show(truncate=100)

    

        
    