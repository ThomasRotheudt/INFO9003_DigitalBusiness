from mrjob.job import MRJob
from mrjob.step import MRStep
import numpy as np

class MatrixMultiplication(MRJob):

    def mapper_init(self):
        # Generate random matrices
        self.matrix1 = np.random.rand(1000, 1000)
        self.matrix2 = np.random.rand(1000, 1000)

    def mapper(self, _, __):
        # Emit elements of matrix A
        for i in range(1000):
            for j in range(1000):
                yield (i, j), ('A', j, self.matrix1[i, j])

        # Emit elements of matrix B
        for j in range(1000):
            for k in range(1000):
                yield (j, k), ('B', j, self.matrix2[j, k])

    def reducer(self, key, values):
        # Initialize variables for matrix multiplication
        result = 0
        a_vals = {}
        b_vals = {}

        # Separate values from both matrices
        for val in values:
            if val[0] == 'A':
                a_vals[val[1]] = val[2]
            else:
                b_vals[val[1]] = val[2]

        # Perform the multiplication
        for j in range(1000):
            result += a_vals.get(j, 0) * b_vals.get(j, 0)

        # Emit the result
        yield key, result

if __name__ == '__main__':
    MatrixMultiplication.run()