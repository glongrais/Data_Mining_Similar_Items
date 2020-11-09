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
        colNames = sigMatrix.columns

        docSimilar = []

        bandSize = 4

        for i in range(0, len(datas), bandSize):
            bucket = {}
            subSet = datas[i:i+bandSize]
            for j in range(len(datas[0])):

                band = [val[j] for val in subSet]
                b = tuple(band)

                h = hash(b)

                if h in bucket:
                    bucket[h].append(colNames[j])
                else:
                    bucket[h] = [colNames[j]]

            for docs in bucket.values():
                if len(docs) > 1 and docs not in docSimilar:
                    docSimilar.append(docs)
        
        return docSimilar
                
