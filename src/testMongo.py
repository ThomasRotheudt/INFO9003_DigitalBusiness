from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col, regexp_replace, lower
from datasets import load_dataset

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

# Collect all words into a list
words_list = df.select("word").rdd.flatMap(lambda x: x).collect()

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

# Print the word frequencies
for word, freq in word_freq.items():
    if freq > 300:
        print(word, freq)

# Stop the SparkSession
spark.stop()






import re
from operator import add
from pyspark import *
from pyspark.sql import *
from datasets import load_dataset
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col, regexp_replace, lower
from datasets import load_dataset

from mrjob.job import MRJob

WORD_RE = re.compile(r"[\w']+")


class MRSparkWordcount(MRJob):

    def spark(self, input_path, output_path):
        # Spark may not be available where script is launched
        from pyspark import SparkContext

        sc = SparkContext(appName='mrjob Spark wordcount script')
    
        # Create a SparkSession
        sk = SparkSession(sc).builder \
            .appName('mrjob Spark wordcount script') \
            .getOrCreate()

        df = sk.read.parquet(input_path)

        lines = df.rdd.take(1)

        counts = (
            lines.flatMap(self.get_words)
            .map(lambda word: (word, 1))
            .reduceByKey(add))

        counts.saveAsTextFile(output_path)

        sk.stop()
        sc.stop()

    def get_words(self, line):
        return WORD_RE.findall(line)


if __name__ == '__main__':
    #MRSparkWordcount.run()
    sc = SparkContext(appName='mrjob Spark wordcount script')
    lines = sc.textFile("input.txt")
    print(lines)
    sk = SparkSession(sc).builder \
            .appName('mrjob Spark wordcount script') \
            .getOrCreate()
    df = sk.read.parquet("input.parquet")
    lines = df.
    print("--------------------")
    print(lines)


    collect()
    -> .txt 
    run()
    -> textFile(.txt)