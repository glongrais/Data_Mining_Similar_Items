from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, ArrayType
import pyspark.sql.functions as F
from pyspark.sql.functions import lit


class CompareSignatures:
   
    def __init__(self, sigMatrix, spark, sc):
        self.sigMatrix = sigMatrix
        self.spark = spark
        self.sc = sc
        self.compare()

    def compare(self):
        datas = self.sigMatrix.collect()
        rows = len(datas)
        cols = len(datas[0])
        simMatrix = []
        
        for i in range(cols):
            tmp = []
            for j in range(cols):
                tmp.append(0)
            simMatrix.append(tmp)

        for i in range(rows):
            for j in range(cols):
                for u in range(cols):
                    if datas[i][j]==datas[i][u] and j!=u:
                        simMatrix[u][j] += 1
        print()
        simMatrix = [[(j/rows) for j in i] for i in simMatrix]
        
        CompareSign = self.spark.createDataFrame(simMatrix, self.sigMatrix.columns)
        #To finish, ADD columns of names
        #names = self.spark.createDataFrame(self.sc.parallelize(self.sigMatrix.columns), StringType(), "names")
        CompareSign.show()
