from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import Row
from pyspark.ml.feature import NGram
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
      
# Driver code 
#word = 'geeks fh kd'
#print(split(word)) 


f = open("Datas/accuracy_garmin_nuvi_255W_gps.txt.data")

letter = split(f)
result =[]
for l in letter :
    result = result + split(l)
#print(result)

df = spark.createDataFrame([Row(inputTokens=result)])
ngram = NGram(n=12)
ngram.setInputCol("inputTokens")
ngram.setOutputCol("nGrams")



#ngram = NGram(n=2, inputCol="inputToken", outputCol="ngrams")

ngramDataFrame = ngram.transform(df)
ngramDataFrame.select("ngrams").show(truncate=False)
#f = open("../Datas/accuracy_garmin_nuvi_255W_gps.txt", "r")
#print(f.read()) 