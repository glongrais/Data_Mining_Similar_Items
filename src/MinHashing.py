from os import listdir
from os.path import isdir, join

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType
from pyspark.ml.feature import NGram, HashingTF, IDF, Tokenizer
from pyspark.ml import Pipeline
from pyspark.ml.linalg import SparseVector, VectorUDT
from pyspark.sql.functions import col
from pyspark.sql import Column

class MinHashing:
   
    def __init__(self, df, spark, sc):
        self.spark = spark
        self.df = df
        self.sc = sc
        self.booleanMatrix()

    def booleanMatrix(self):
        SetShingling = set()
        Rows = self.df.count()
        for i in range(Rows):
            SetShingling = SetShingling.union(set(self.df.collect()[i]["features"].indices))
        Setint = set()
        Setint = [int(i) for i in SetShingling]
        #print(SetShingling)

        Matrix = self.spark.createDataFrame(self.sc.parallelize(Setint), IntegerType())
        Matrix.show()

        for i in range(Rows):
            listsh = set(ngramDataFrame.collect()[i]["ngrams"]) # get each doc shingles
            listcontains = F.udf(lambda value: value in listsh) # get if this shingle is in the list
            shinglesDf = shinglesDf.withColumn("document "+str(i), listcontains(F.col("shingles")))
        #    Shingle = set(df.collect()[i]["features"])
        #    if 
