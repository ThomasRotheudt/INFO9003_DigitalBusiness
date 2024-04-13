from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col, regexp_replace, lower
from datasets import load_dataset
import time

# Create a SparkSession
spark = SparkSession.builder \
    .appName("WordFrequencyCount") \
    .getOrCreate()

# Specify the name of the dataset
dataset_name = "stanfordnlp/imdb"

# Load the dataset using the `datasets` library
# Note: You need to install the `datasets` library if you haven't already

dataset = load_dataset(dataset_name)

# Convert the dataset to a PySpark DataFrame
# Assuming the text is stored in a column named 'text'
df = spark.createDataFrame(dataset['train'])

# Remove punctuations and convert to lowercase
df = df.withColumn("text", lower(regexp_replace(col("text"), "[^a-zA-Z0-9\-]+", " ")))

# Tokenize the text into words
df = df.withColumn("words", split(col("text"), " "))

# Explode the array of words into separate rows
df = df.select(explode(col("words")).alias("word"))

# Filter out empty words
df = df.filter(col("word") != "")


#start timer
start = time.time()

# Map each word to a (word, 1) pair
word_counts = df.rdd.map(lambda word: (word, 1))

# Reduce by key to sum the counts for each word
word_freq = word_counts.reduceByKey(lambda a, b: a + b).collectAsMap()

#end timer
end = time.time()

# Print the time taken to compute the word frequencies
print("Time taken:", end - start, "seconds")

# Print the word frequencies
for word, freq in word_freq.items():
    if freq > 300:
        print(word, freq)

# Stop the SparkSession
spark.stop()
