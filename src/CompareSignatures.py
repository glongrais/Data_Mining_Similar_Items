from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, ArrayType
import pyspark.sql.functions as F
from pyspark.sql.window import Window


class CompareSignatures:
   
    def __init__(self, sigMatrix, spark, sc):
        self.sigMatrix = sigMatrix
        self.spark = spark
        self.sc = sc
        
#Create the matrix showing the similarity between documents using the signature matrix
    def compare(self):
        datas = self.sigMatrix.collect()
        rows = len(datas)
        cols = len(datas[0])
        simMatrix = []
        
        #Create the matrix of similarity
        for i in range(cols):
            tmp = []
            for j in range(cols):
                tmp.append(0)
            simMatrix.append(tmp)

        #Add 1 if two documents has the same hash for a specific shingling
        for i in range(rows):
            for j in range(cols):
                for u in range(cols):
                    if datas[i][j]==datas[i][u] and j!=u:
                        simMatrix[u][j] += 1
        
        simMatrix = [[(j/rows) for j in i] for i in simMatrix]#Final computation for similarity matrix using signatures
        

        CompareSign = self.spark.createDataFrame(simMatrix, self.sigMatrix.columns)
        #Retreive files names as dataframe
        names = self.spark.createDataFrame(self.sc.parallelize(self.sigMatrix.columns), StringType(), "names")
        #Put names as a new row in both dataframe and join them
        names=names.withColumn('row_index', F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))
        CompareSign=CompareSign.withColumn('row_index', F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))
        CompareSign = CompareSign.join(names, on=["row_index"]).drop("row_index")
        
        CompareSign.show(truncate=20)
        return CompareSign
