from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import Row
from pyspark.ml.feature import NGram
from pyspark.ml.feature import HashingTF
sc = SparkContext('local')
spark = SparkSession(sc)

class Shingling:
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


#cl = Shingling("Datas/accuracy_garmin_nuvi_255W_gps.txt.data",4)


# Python3 program to Split string into characters 
def split(word): 
    return [char for char in word]  

#Open text file
f = open("Datas/accuracy_garmin_nuvi_255W_gps.txt.data")

#Split the text into char array
letter = split(f)
result =[]
for l in letter :
    result = result + split(l)
#print(result)

#Datafram creation
df = spark.createDataFrame([Row(inputTokens=result)])

#ngram creation
#ngram = NGram(n=8)
#ngram.setInputCol("inputTokens")
#ngram.setOutputCol("nGrams")
ngram = NGram(n=8, inputCol="inputTokens", outputCol="ngrams")
ngramDataFrame = ngram.transform(df)
#ngramDataFrame.select("ngrams").show(truncate=100)


##TRY TO HASH (Pas foufou pour le moment)
hashingTF = HashingTF(inputCol="ngrams", outputCol="features")
hashingTF.setNumFeatures(10)

hashingTF.transform(ngramDataFrame).head().features
#SparseVector(10, {5: 1.0, 7: 1.0, 8: 1.0})

hashingTF.setParams(outputCol="freqs").transform(ngramDataFrame).head().freqs
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