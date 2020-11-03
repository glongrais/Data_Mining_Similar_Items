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

    def jaccard(self):

        s1 = set()
        s2 = set()

        datas = self.df.select("id","features").collect()

        schema = StructType([StructField("id", IntegerType(), True), StructField("jaccardSimilarity", VectorUDT(), True)])
        tmpDf = self.spark.createDataFrame([],schema)
       
        for t in datas:

            vect = {}
            tmpT = set(t.features.indices)
        
            for u in datas:
                tmpU = set(u.features.indices)
                s1 = tmpT.union(tmpU)
                s2 = tmpT.intersection(tmpU)
                try:
                    vect[u.id] = len(s2)/len(s1)
                except ZeroDivisionError:
                    vect[u.id] = 0

            sparceVect = SparseVector(len(vect), vect)
            tmpDf = tmpDf.union(self.spark.createDataFrame([(t.id, sparceVect)], schema))
          
        self.df = self.df.join(tmpDf, on=["id"])

        self.df.select("id", "jaccardSimilarity").show(truncate=200)

    

        
    