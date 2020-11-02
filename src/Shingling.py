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

#cl = Shingling("Datas/accuracy_garmin_nuvi_255W_gps.txt.data",4)

"""
# Python3 program to Split string into characters 
def split(word): 
    return [char for char in word]  

#Open text file
#f = open("Datas/accuracy_garmin_nuvi_255W_gps.txt.data")
f = open("Datas/test.data")
data = f.read()

#Split the text into char array
#letter = split(f)
#result =[]
#for l in letter :
#    result = result + split(l)
#print(result)

#Datafram creation
df = spark.createDataFrame([Row(inputTokens=data)])
tokenizer = Tokenizer(inputCol="inputTokens", outputCol="words")
tokenized = tokenizer.transform(df)
tokenized.select("inputTokens", "words").show(truncate=100)
#ngram creation
#ngram = NGram(n=8)
#ngram.setInputCol("inputTokens")
#ngram.setOutputCol("nGrams")
ngram = NGram(n=2, inputCol="words", outputCol="ngrams")
ngramDataFrame = ngram.transform(tokenized)
#ngramDataFrame.select("ngrams").show(truncate=100)


##TRY TO HASH (Pas foufou pour le moment)
hashingTF = HashingTF(inputCol="ngrams", outputCol="rawFeatures")
hashingTF.setNumFeatures(150)

featurizedData =  hashingTF.transform(ngramDataFrame)
#SparseVector(10, {5: 1.0, 7: 1.0, 8: 1.0})

idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)

rescaledData.select("ngrams", "features").show(truncate=100)

for row in rescaledData.select("features").collect():
    print(row[0].toArray())

#hashingTF.setParams(outputCol="freqs").transform(ngramDataFrame).head().freqs
#SparseVector(10, {5: 1.0, 7: 1.0, 8: 1.0})

#params = {hashingTF.numFeatures: 5, hashingTF.outputCol: "vector"}

#hashingTF.transform(ngramDataFrame, params).head().vector
#SparseVector(5, {0: 1.0, 2: 1.0, 3: 1.0})

#hashingTFPath = temp_path + "/hashing-tf"

#hashingTF.save(hashingTFPath)

#loadedHashingTF = HashingTF.load(hashingTFPath)

#loadedHashingTF.getNumFeatures() == hashingTF.getNumFeatures()
#True

#loadedHashingTF.transform(ngramDataFrame).take(1) == hashingTF.transform(ngramDataFrame).take(1)
#True

#hashingTF.indexOf("b")
#5



#f = open("../Datas/accuracy_garmin_nuvi_255W_gps.txt", "r")
#print(f.read()) """