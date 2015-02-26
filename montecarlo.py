import sys
from random import random
from math import sin
from operator import add

from pyspark import SparkContext


if __name__ == "__main__":
	"""
        Usage: montecarlo [x_min] [x_max] [partitions]
    """
	sc = SparkContext(appName="PythonMonteCarlo")
	x_min = int(sys.argv[1]) if len(sys.argv) > 1 else 0
	x_max = int(sys.argv[2]) if len(sys.argv) > 2 else 1
	partitions = int(sys.argv[3]) if len(sys.argv) > 3 else 2
	number_of_tries = 10000 * partitions
	y_min = 0
	y_max = 1

	def check_random_point(x_min, x_max):
		x_random = x_min+(x_max-x_min)*random()
		y_random = y_min + (y_max - y_min)*random()
		return 1 if y_random < sin_squared_function(x_random) else 0
		return 1

	def sin_squared_function(x):
		return sin(x) * sin(x)    
    
	count = sc.parallelize(xrange(1, number_of_tries + 1), partitions).map(lambda _: check_random_point(x_min, x_max)).reduce(add)
	integral = (x_max-x_min)*y_max*float(count)/float(number_of_tries)   
	print "Definite integral for sin(x)^2 function in interval [%d; %d] is roughly %f" % (x_min, x_max, integral)
	sc.stop()
	
