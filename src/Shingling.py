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

f = sc.textFile("Datas/accuracy_garmin_nuvi_255W_gps.txt.data")
words = f.map(lambda l : l.split(' '))
wordDataFrame = words.toDF

ngram = NGram(n=2, inputCol="inputToken", outputCol="ngrams")

ngramDataFrame = ngram.transform(wordDataFrame)
ngramDataFrame.select("ngrams").show(truncate=False)
#f = open("../Datas/accuracy_garmin_nuvi_255W_gps.txt", "r")
#print(f.read()) 