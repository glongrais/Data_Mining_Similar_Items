from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, ArrayType
import pyspark.sql.functions as F
from pyspark.sql.window import Window


class CompareSignatures:
   
    def __init__(self, sigMatrix, spark, sc):
        self.sigMatrix = sigMatrix
        self.spark = spark
        self.sc = sc
        

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
        #Retreive files names
        names = self.spark.createDataFrame(self.sc.parallelize(self.sigMatrix.columns), StringType(), "names")
        #Put names as a new row
        names=names.withColumn('row_index', F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))
        CompareSign=CompareSign.withColumn('row_index', F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))
        CompareSign = CompareSign.join(names, on=["row_index"]).drop("row_index")
        
        CompareSign.show(truncate=20)
        return CompareSign
