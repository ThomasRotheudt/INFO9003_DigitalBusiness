import numpy as np
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from pyspark.sql.types import ArrayType, FloatType

# Create Spark session
spark = SparkSession.builder \
    .appName("MatrixMultiplication") \
    .getOrCreate()

# Generate two random matrices
matrix_size = 5
matrix1 = np.random.randint(10, size=(matrix_size, matrix_size))
matrix2 = np.random.randint(10, size=(matrix_size, matrix_size))

# Numpy version
start_time = time.time()
result_matrix2 = np.dot(matrix1, matrix2)
end_time = time.time()
print(f"Numpy version time: {end_time - start_time} seconds")

# PySpark version
# Convert matrices to RDDs of key-value pairs
matrix1_rdd = spark.sparkContext.parallelize([((i, j), matrix1[i][j]) for i in range(matrix_size) for j in range(matrix_size)])
matrix2_rdd = spark.sparkContext.parallelize([((j, k), matrix2[j][k]) for j in range(matrix_size) for k in range(matrix_size)])

start_time = time.time()

# Map Step: Group elements of matrix1 by row and elements of matrix2 by column
matrix1_grouped = matrix1_rdd.map(lambda x: (x[0][1], (x[0][0], x[1])))
matrix2_grouped = matrix2_rdd.map(lambda x: (x[0][0], (x[0][1], x[1])))

# Join matrices by matching row and column indices
joined_matrices = matrix1_grouped.join(matrix2_grouped)

# Multiply corresponding elements and emit key-value pairs
mapped_pairs = joined_matrices.map(lambda x: ((x[1][0][0], x[1][1][0]), x[1][0][1] * x[1][1][1]))

# Aggregate values for each key (i, k)
result_rdd = mapped_pairs.reduceByKey(lambda x, y: x + y)

end_time = time.time()

""" # Collect and display the resulting matrix
result_list = result_rdd.collect()

# Collect and display the resulting matrix
result_matrix = np.zeros((matrix_size, matrix_size))
for ((i, j), value) in result_list:
    result_matrix[i][j] = value


print(result_matrix)

print(result_matrix2) """

print(f"PySpark version time: {end_time - start_time} seconds")

# Stop Spark session
spark.stop()
