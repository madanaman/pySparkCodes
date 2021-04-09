import pyspark
from pyspark import SparkContext
sc = SparkContext()
myRDD = [('User1', 30), ('User2', 40), ('User3', 50)]
myRDD.take(1)