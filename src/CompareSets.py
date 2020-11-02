from os import listdir
from os.path import isdir, join

from pyspark.sql.types import StructField, StructType, StringType, IntegerType
from pyspark.ml.feature import NGram, HashingTF, IDF, Tokenizer
from pyspark.ml import Pipeline

class CompareSets:
   
    def __init__(self, df, spark):
        self.spark = spark
        self.df = df
        self.jaccard()

    def jaccard(self):
        #def jaccard(list1, list2):
        self.df.filter(self.df.id == 1).intersect(self.df.filter(self.df.id == 0)).show(truncate=100)   #len(list(set(list1).intersection(list2)))
        #union = (len(list1) + len(list2)) - intersection
        #return float(intersection) / union
    