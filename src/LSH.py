from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, ArrayType
from pyspark.ml.feature import NGram, HashingTF, IDF, Tokenizer
from pyspark.ml import Pipeline
from pyspark.ml.linalg import SparseVector, VectorUDT
import pyspark.sql.functions as F
from pyspark.sql import Column

class LSH:

    def __init__(self, spark, sc):
        self.spark  = spark
        self.sc = sc

    # Return the potential similar items
    def lsh(self, sigMatrix, b):
        datas = sigMatrix.collect()
        colNames = sigMatrix.columns

        docSimilar = []

        bandSize = b

        for i in range(0, len(datas), bandSize): # Loop throw each bands
            bucket = {} # Create a new bucket for each band
            subSet = datas[i:i+bandSize] # Select the subset corresponding to the band
            for j in range(len(datas[0])): # Loop throw each vector of the band

                band = [val[j] for val in subSet] # Select a specific vector
                b = tuple(band) # Transform the vector in tuple

                h = hash(b) # Hash the tuple

                if h in bucket: # if the hash is already in the bucket, we add the new file to it
                    bucket[h].append(colNames[j])
                else:
                    bucket[h] = [colNames[j]]

            for docs in bucket.values(): # add all the potential similar items into a list
                if len(docs) > 1:
                    tmp = []
                    for p1 in range(len(docs)): # if more than 2 similar items we distribute them in pair (e.g [1, 2, 3] -> [1, 2], [2, 3], [1, 3])
                        for p2 in range(p1+1, len(docs)):
                            tmp.append([docs[p1], docs[p2]])

                    for val in tmp:
                        if val not in docSimilar:
                            docSimilar.append(val)

        
        return docSimilar
                
