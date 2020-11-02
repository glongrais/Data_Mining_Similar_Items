from os import listdir
from os.path import isdir, join

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType
from pyspark.ml.feature import NGram, HashingTF, IDF, Tokenizer
from pyspark.ml import Pipeline
import pyspark.sql.functions as F
from pyspark.sql import Column


class CompareSets:
   
    def __init__(self, df, spark):
        self.spark = spark
        self.df = df
        self.jaccard()

    def jaccard(self):
        #def jaccard(list1, list2):
        #self.df.filter(self.df.id == 1).show(truncate=100) 
        #self.df.filter(self.df.id == 0).show(truncate=100)  #len(list(set(list1).intersection(list2)))
        #union = (len(list1) + len(list2)) - intersection
        #return float(intersection) / union


        s1 = set()
        s2 = set()
        first = True
        for t in self.df.select("id","features").collect():
            tmpT = set(t.features.indices)
            schema = StructType([StructField(str(t.id), FloatType(), True)])
            tmpDf = self.spark.createDataFrame([],schema)
            l = []
            for u in self.df.select("id","features").collect():
                tmpU = set(u.features.indices)
                s1 = tmpT.union(tmpU)
                s2 = tmpT.intersection(tmpU)
                tmpDf = tmpDf.union(self.spark.createDataFrame([(len(s2)/len(s1))],[str(t.id)]))
                #l += [len(s2)/len(s1)]
                #([(t.id, len(s2)/len(s1))], ["id", str(u.id) ])
            self.df.withColumn(t.id,tmpDf(str(t.id)))
        self.df.show(truncate=100)

    

        
    