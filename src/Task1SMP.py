import re
from datasets import load_dataset
from pyspark.sql import SparkSession
import time

# Create Spark session
spark = SparkSession.builder \
    .appName("WordCount") \
    .getOrCreate()

# Dataset RDD
datasetName = "imdb"
dataset = load_dataset(datasetName)

# Convert the dataset into a Spark RDD
rdd = spark.sparkContext.parallelize(dataset['train']['text'])

# Regular expression pattern to match words
pattern = re.compile(r"\w+")

# Start the timer for flatMap operation
timer = time.time()

# Extract words from RDD using flatMap
words = rdd.flatMap(lambda x: pattern.findall(x))

# Calculate the time taken for flatMap operation
timer = time.time() - timer

# Collect the words into a list
words_list = words.collect()

# Start the timer for word frequency calculation
timer1 = time.time()

# Initialize an empty dictionary to store the word frequencies
word_freq = {}

# Iterate through every word one by one
for word in words_list:
    # If the word is not in the dictionary, add it with a count of 1
    if word not in word_freq:
        word_freq[word] = 1
    # If the word is already in the dictionary, increment its count
    else:
        word_freq[word] += 1

# Calculate the time taken for word frequency calculation
timer1 = time.time() - timer1

# Print the total time taken for both operations
print("\n\n------------------ Timer ------------------")
print("|    time for map reduce: {:.2f} seconds    |".format(timer + timer1))
print("-------------------------------------------\n\n")
