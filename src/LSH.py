from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, ArrayType
from pyspark.ml.feature import NGram, HashingTF, IDF, Tokenizer
from pyspark.ml import Pipeline
from pyspark.ml.linalg import SparseVector, VectorUDT
import pyspark.sql.functions as F
from pyspark.sql import Column

class LSH:

    def __init__(self, spark, sc):
        self.spark  = spark
        self.sc = sc

    def lsh(self, sigMatrix):
        datas = sigMatrix.collect()

        docSimilar = []

        bandSize = 4

        for i in range(0, len(datas), bandSize):
            print(i)