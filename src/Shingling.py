from os import listdir
from os.path import isdir, join

from pyspark.sql.types import StructField, StructType, StringType, IntegerType
from pyspark.ml.feature import NGram, HashingTF, IDF, Tokenizer
from pyspark.ml import Pipeline

class Shingling:

    def __init__(self, folderPath, k, spark):
        self.spark = spark
        self.k = k
        self.folderPath = folderPath
    
    def getNGram(self):

        df = self.readData(self.folderPath)

        tokenizer = Tokenizer(inputCol="inputTokens", outputCol="words")
        ngram = NGram(n=self.k, inputCol="words", outputCol="ngrams")
        hashingTF = HashingTF(inputCol="ngrams", outputCol="rawFeatures")
        idf = IDF(inputCol="rawFeatures", outputCol="features")

        model = Pipeline(stages=[tokenizer, ngram, hashingTF, idf]).fit(df)

        result = model.transform(df)

        return result


        
    
    def readData(self, folderPath):
        index = 0
        first = True

        schema = StructType([StructField("id", IntegerType(), True), StructField("inputTokens", StringType(), True)])

        df = self.spark.createDataFrame([], schema)

        for f in listdir(folderPath): 
            
            if not isdir(f): 
                
                fullPath = join(folderPath, f)
                dataFile = open(fullPath, encoding="ISO-8859-1")
                data = dataFile.read()
                if first:
                    df = self.spark.createDataFrame([(index, data)], schema)
                    first = False
                else:
                    df = df.union(self.spark.createDataFrame([(index, data)], schema))
                index += 1
        return df
        