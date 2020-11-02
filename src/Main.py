from Shingling import Shingling

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import Row
from pyspark.ml.feature import NGram, HashingTF, IDF, Tokenizer
sc = SparkContext('local[*]')
spark = SparkSession(sc)

shin = Shingling("Datas",10,spark)
shin.getNGram().select("id", "features").show(truncate=100)