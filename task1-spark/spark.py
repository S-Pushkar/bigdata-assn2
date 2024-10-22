#!/usr/bin/env python3

import sys
import pyspark
from pyspark import SQLContext

sc = pyspark.SparkContext('local[*]')

spark = SQLContext.getOrCreate(sc)

scores = {
    2012: {
        'Gold': 20,
        'Silver': 15,
        'Bronze': 10
    },
    2016: {
        'Gold': 12,
        'Silver': 8,
        'Bronze': 6
    },
    2020: {
        'Gold': 15,
        'Silver': 12,
        'Bronze': 7
    }
}

athlete_2012 = spark.read.csv(sys.argv[1], header=True, inferSchema=True)

athlete_2016 = spark.read.csv(sys.argv[2], header=True, inferSchema=True)

athlete_2020 = spark.read.csv(sys.argv[3], header=True, inferSchema=True)

coaches = spark.read.csv(sys.argv[4], header=True, inferSchema=True)

medals = spark.read.csv(sys.argv[5], header=True, inferSchema=True)

output_file = sys.argv[6]


