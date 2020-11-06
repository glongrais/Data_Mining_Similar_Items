from Shingling import Shingling
from CompareSets import CompareSets
from MinHashing import MinHashing

import numpy as np
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import Row
from pyspark.ml.feature import NGram, HashingTF, IDF, Tokenizer
sc = SparkContext('local[*]')
spark = SparkSession(sc)

shin = Shingling("Datas2",2,spark)
df = shin.getNGram()#.select("id","features").show(truncate=100)

#print(df.collect()[0]["rawFeatures"])

#comp = CompareSets(df, spark)

min = MinHashing(df,spark,sc)