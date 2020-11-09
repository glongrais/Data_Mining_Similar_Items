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

        #Prepare the stages to build the pipeline
        tokenizer = Tokenizer(inputCol="inputTokens", outputCol="words")#Tokenize the separate the text in words
        ngram = NGram(n=self.k, inputCol="words", outputCol="ngrams") #perform NGram and put the results in ngrams column
        hashingTF = HashingTF(inputCol="ngrams", outputCol="rawFeatures", numFeatures=10000)#apply a hashfunction to the ngrams colums
        idf = IDF(inputCol="rawFeatures", outputCol="features")#Rescale vector rawFeatures to improve performance

        model = Pipeline(stages=[tokenizer, ngram, hashingTF, idf]).fit(df)#Build the pipeline

        result = model.transform(df)#run the pipeline

        return result


        
    
    def readData(self, folderPath):
        index = 0
        first = True

        #Schema creation to collect the docs name and the content
        schema = StructType([StructField("id", IntegerType(), True),StructField("docName", StringType(), True), StructField("inputTokens", StringType(), True)])

        df = self.spark.createDataFrame([], schema)#creation of a dataframe with previous schema

        for f in listdir(folderPath): #for all file in folder
            
            if not isdir(f): 
                
                fullPath = join(folderPath, f)
                dataFile = open(fullPath, encoding="ISO-8859-1")
                data = dataFile.read() #get the datas
                if first:
                    df = self.spark.createDataFrame([(index, f, data)], schema)
                    first = False
                else:
                    df = df.union(self.spark.createDataFrame([(index, f, data)], schema)) #Create the datagrame with doc and content 
                    index += 1
        return df
        