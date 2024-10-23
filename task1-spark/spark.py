#!/usr/bin/env python3

import sys
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("Olympics").getOrCreate()

def convert_to_uppercase(df):
    for col_name, dtype in df.dtypes:
        if dtype == 'string':
            df = df.withColumn(col_name, F.upper(F.col(col_name)))
    return df

athlete_2012 = spark.read.csv(sys.argv[1], header=True, inferSchema=True)
athlete_2012 = convert_to_uppercase(athlete_2012)
athlete_2012.createOrReplaceTempView('athlete_2012')

athlete_2016 = spark.read.csv(sys.argv[2], header=True, inferSchema=True)
athlete_2016 = convert_to_uppercase(athlete_2016)
athlete_2016.createOrReplaceTempView('athlete_2016')

athlete_2020 = spark.read.csv(sys.argv[3], header=True, inferSchema=True)
athlete_2020 = convert_to_uppercase(athlete_2020)
athlete_2020.createOrReplaceTempView('athlete_2020')

coaches = spark.read.csv(sys.argv[4], header=True, inferSchema=True)
coaches = convert_to_uppercase(coaches)
coaches.createOrReplaceTempView('coaches')

medals = spark.read.csv(sys.argv[5], header=True, inferSchema=True)
medals = convert_to_uppercase(medals)
medals.createOrReplaceTempView('medals')

output_file = sys.argv[6]

# Task 1.1

athletes = spark.sql('''
    SELECT DISTINCT * FROM athlete_2012
    UNION
    (SELECT DISTINCT * FROM athlete_2016
    UNION
    SELECT DISTINCT * FROM athlete_2020)
''')

athletes.createOrReplaceTempView('athletes')

athletes_join_medals = spark.sql('''
    SELECT a.id AS id, a.name AS name, a.sport AS sport, m.year AS year, m.medal AS medal,
       CASE
           WHEN m.medal = 'GOLD' AND m.year = 2012 THEN 20
           WHEN m.medal = 'SILVER' AND m.year = 2012 THEN 15
           WHEN m.medal = 'BRONZE' AND m.year = 2012 THEN 10
           WHEN m.medal = 'GOLD' AND m.year = 2016 THEN 12
           WHEN m.medal = 'SILVER' AND m.year = 2016 THEN 8
           WHEN m.medal = 'BRONZE' AND m.year = 2016 THEN 6
           WHEN m.medal = 'GOLD' AND m.year = 2020 THEN 15
           WHEN m.medal = 'SILVER' AND m.year = 2020 THEN 12
           WHEN m.medal = 'BRONZE' AND m.year = 2020 THEN 7
           ELSE 0
       END AS score,
       CASE
           WHEN m.medal = 'GOLD' THEN 1
           ELSE 0
       END AS GOLD,
       CASE 
           WHEN m.medal = 'SILVER' THEN 1
           ELSE 0
       END AS SILVER,
       CASE
           WHEN m.medal = 'BRONZE' THEN 1
           ELSE 0
       END AS BRONZE
    FROM athletes a
    JOIN medals m ON a.id = m.id AND a.sport = m.sport
    GROUP BY a.id, a.name, a.sport, m.year, m.medal
    ORDER BY a.name
''')

athletes_join_medals.createOrReplaceTempView('athletes_join_medals')

athlete_total_scores = spark.sql('''
    SELECT id, name, sport, SUM(score) AS total_score, SUM(GOLD) AS GOLD, SUM(SILVER) AS SILVER, SUM(BRONZE) AS BRONZE
    FROM athletes_join_medals
    GROUP BY id, name, sport
    ORDER BY total_score DESC
''')

athlete_total_scores.createOrReplaceTempView('athlete_total_scores')

athlete_rank = spark.sql('''
    SELECT sport, name, total_score, GOLD, SILVER, BRONZE,
    ROW_NUMBER() OVER (PARTITION BY sport ORDER BY total_score DESC, GOLD DESC, SILVER DESC, BRONZE DESC, name ASC) AS rank
    FROM athlete_total_scores
''')

athlete_rank.createOrReplaceTempView('athlete_rank')

spark.sql('''
    SELECT sport, name, total_score, GOLD, SILVER, BRONZE
    FROM athlete_rank
    WHERE rank = 1
''').show()

# Task 1.2


