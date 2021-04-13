from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext("local[2]", "statefulCounts")

ssc = StreamingContext(sparkContext=sc, batchDuration=10)

ssc.checkpoint("checkpoint")
# Define updateFunc: sum of the (key, value) pairs
def updateFunc(new_values, last_sum):
   return sum(new_values) + (last_sum or 0)


lines = ssc.socketTextStream(hostname="localhost", port=9999)

running_counts = lines.flatMap(lambda line: line.split(" "))\
          .map(lambda word: (word, 1))\
          .updateStateByKey(updateFunc)
# Print the first ten elements of each RDD generated in this stateful DStream to the console
running_counts.pprint()

# Start the computation
ssc.start()

# Wait for the computation to terminate
ssc.awaitTermination()
