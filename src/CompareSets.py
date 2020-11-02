from os import listdir
from os.path import isdir, join

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType
from pyspark.ml.feature import NGram, HashingTF, IDF, Tokenizer
from pyspark.ml import Pipeline
from pyspark.sql.functions import col
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
        #print(self.df['id'])
        for t in self.df.select("id","features").collect():
            tmpT = set(t.features.indices)
            schema = StructType([StructField("id", IntegerType(), True), StructField(str(t.id), FloatType(), True)])
            tmpDf = self.spark.createDataFrame([],schema)
        
            for u in self.df.select("id","features").collect():
                tmpU = set(u.features.indices)
                s1 = tmpT.union(tmpU)
                s2 = tmpT.intersection(tmpU)
                try:
                    tmpDf = tmpDf.union(self.spark.createDataFrame([(u.id, len(s2)/len(s1))],[str(t.id)]))
                except ZeroDivisionError:
                    tmpDf = tmpDf.union(self.spark.createDataFrame([(u.id, len(s2))],[str(t.id)]))
                
                #l += [len(s2)/len(s1)]
                #([(t.id, len(s2)/len(s1))], ["id", str(u.id) ])
            self.df = self.df.join(tmpDf, on=["id"])
        self.df.show(truncate=10)

    

        
    