from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import Row
from pyspark.ml.feature import NGram, HashingTF, IDF, Tokenizer
sc = SparkContext('local')
spark = SparkSession(sc)

class CompareSets:
    def __init__(self, name, k):
        self.doc = name
        self.k   = k
        print(self.doc)
        with open(self.doc) as f:
            while True:
                c = f.read(1)
                if not c:
                    #print("End of file")
                    break
                #print("Read a character:", c)



file1  = open("Datas/test.data")
data1 = file1.read()

file2  = open("Datas/test2.data")
data2 = file2.read()



#Datafram creation
df = spark.createDataFrame([
    (0, data1),
    (1, data2)]
    , ["id", "inputTokens"])


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

rescaledData.select("features").show(truncate=100)
   

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
#print(f.read()) 