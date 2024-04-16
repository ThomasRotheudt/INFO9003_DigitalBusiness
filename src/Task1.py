import re
from datasets import load_dataset
from pyspark.sql import SparkSession 
import time
import matplotlib.pyplot as plt

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

# Start the timer
timer = time.time()

# Apply map and flatMap to perform MapReduce
found_words_rdd = (
    rdd.flatMap(lambda line: pattern.findall(line))  # Split each line into words
    .map(lambda word: (word, 1))  # Map each word to a tuple (word, 1)
    .reduceByKey(lambda a, b: a + b)  # Reduce by key (word), summing up the counts
)

# Calculate the time taken for MapReduce operation
timer = time.time() - timer

# Collect the results as a list
word_counts = found_words_rdd.collect()

# Sort the words by number of occurrences in descending order
sorted_word_counts = sorted(word_counts, key=lambda x: x[1], reverse=True)

# Separate the top 10 words and their counts
top_words = [x[0] for x in sorted_word_counts[:10]]
word_counts_top = [x[1] for x in sorted_word_counts[:10]]

# Create a bar chart of the most frequent words
plt.figure(figsize=(10, 6))
plt.bar(top_words, word_counts_top, color='skyblue')
plt.xlabel('Words')
plt.ylabel('Number of Occurrences')
plt.title('Top 20 Most Frequent Words')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()

# Print the time taken for MapReduce operation
print("\n\n------------------ Timer ------------------")
print("|    time for map reduce: {:.2f} seconds    |".format(timer))
print("-------------------------------------------\n\n")

# Stop Spark session
spark.stop()
