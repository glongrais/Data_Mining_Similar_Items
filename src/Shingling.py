import pyspark as spark
from pyspark.ml.feature import NGram

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




cl = Shingling("Datas/accuracy_garmin_nuvi_255W_gps.txt.data",4)
wordDataFrame = spark.createDataFrame([
    (0, ["Hi", "I", "heard", "about", "Spark"]),
    (1, ["I", "wish", "Java", "could", "use", "case", "classes"]),
    (2, ["Logistic", "regression", "models", "are", "neat"])
], ["id", "words"])

ngram = NGram(n=2, inputCol="words", outputCol="ngrams")

ngramDataFrame = ngram.transform(wordDataFrame)
ngramDataFrame.select("ngrams").show(truncate=False)
#f = open("../Datas/accuracy_garmin_nuvi_255W_gps.txt", "r")
#print(f.read()) 