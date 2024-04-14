import re
from datasets import load_dataset
from pyspark.sql import SparkSession
import time

# Create Spark session
spark = SparkSession.builder \
    .appName("WordCount") \
    .getOrCreate()

# Sample RDD

datasetName = "stanfordnlp/imdb"
dataset = load_dataset(datasetName)

# Convertir le dataset en un RDD Spark
rdd = spark.sparkContext.parallelize(dataset['train']['text'])

# Regular expression pattern
pattern = re.compile(r"\w+")

timer = time.time()
# Apply map and flatMap to perform MapReduce
found_words_rdd = (
    rdd.flatMap(lambda line: pattern.findall(line))
    .map(lambda word: (word, 1))
    .reduceByKey(lambda a, b: a + b)
)
timer = time.time() - timer

# Collect the results as a list
word_counts = found_words_rdd.collect()

# Print the results
for word, count in word_counts:
    if count > 300:
        print(f"{word}: {count}")

print(timer)
# Stop Spark session
spark.stop()