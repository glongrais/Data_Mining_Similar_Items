from Shingling import Shingling
from CompareSets import CompareSets


from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import Row
from pyspark.ml.feature import NGram, HashingTF, IDF, Tokenizer
sc = SparkContext('local[*]')
spark = SparkSession(sc)

shin = Shingling("Datas",6,spark)
df = shin.getNGram()

comp = CompareSets(df, spark)