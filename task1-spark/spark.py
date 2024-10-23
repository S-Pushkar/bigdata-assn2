#!/usr/bin/env python3

import sys
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("Olympics").getOrCreate()

def convert_to_uppercase(df):
    for col_name, dtype in df.dtypes:
        if dtype == 'string':
            df = df.withColumn(col_name, F.upper(F.col(col_name)))
    return df

athlete_2012 = spark.read.csv(sys.argv[1], header=True, inferSchema=True).withColumn('year', F.lit(2012))
athlete_2012 = convert_to_uppercase(athlete_2012)
athlete_2012.createOrReplaceTempView('athlete_2012')

athlete_2016 = spark.read.csv(sys.argv[2], header=True, inferSchema=True).withColumn('year', F.lit(2016))
athlete_2016 = convert_to_uppercase(athlete_2016)
athlete_2016.createOrReplaceTempView('athlete_2016')

athlete_2020 = spark.read.csv(sys.argv[3], header=True, inferSchema=True).withColumn('year', F.lit(2020))
athlete_2020 = convert_to_uppercase(athlete_2020)
athlete_2020.createOrReplaceTempView('athlete_2020')

coaches = spark.read.csv(sys.argv[4], header=True, inferSchema=True)
coaches = convert_to_uppercase(coaches)
coaches.createOrReplaceTempView('coaches')

medals = spark.read.csv(sys.argv[5], header=True, inferSchema=True)
medals = convert_to_uppercase(medals)
medals.createOrReplaceTempView('medals')

medals = spark.sql('''
    SELECT * FROM medals
    WHERE year IN (2012, 2016, 2020)
''')

output_file = sys.argv[6]

# Task 1.1

athletes = athlete_2012.union(athlete_2016).union(athlete_2020)
athletes.createOrReplaceTempView('athletes')

athletes_join_medals = athletes.join(medals, (athletes.id == medals.id) & (athletes.sport == medals.sport) & (athletes.event == medals.event) & (athletes.year == medals.year), 'inner').select(athletes.id, athletes.name, athletes.sport, medals.year, medals.medal)
athletes_join_medals.createOrReplaceTempView('athletes_join_medals')

athletes_join_medals = athletes_join_medals.withColumn('score', F.when((F.col('medal') == 'GOLD') & (F.col('year') == 2012), 20).when((F.col('medal') == 'SILVER') & (F.col('year') == 2012), 15).when((F.col('medal') == 'BRONZE') & (F.col('year') == 2012), 10).when((F.col('medal') == 'GOLD') & (F.col('year') == 2016), 12).when((F.col('medal') == 'SILVER') & (F.col('year') == 2016), 8).when((F.col('medal') == 'BRONZE') & (F.col('year') == 2016), 6).when((F.col('medal') == 'GOLD') & (F.col('year') == 2020), 15).when((F.col('medal') == 'SILVER') & (F.col('year') == 2020), 12).when((F.col('medal') == 'BRONZE') & (F.col('year') == 2020), 7).otherwise(0))

athletes_join_medals = athletes_join_medals.withColumn('GOLD', F.when(F.col('medal') == 'GOLD', 1).otherwise(0))

athletes_join_medals = athletes_join_medals.withColumn('SILVER', F.when(F.col('medal') == 'SILVER', 1).otherwise(0))

athletes_join_medals = athletes_join_medals.withColumn('BRONZE', F.when(F.col('medal') == 'BRONZE', 1).otherwise(0))

athletes_join_medals.createOrReplaceTempView('athletes_join_medals')

athletes_join_medals = spark.sql('''
    SELECT id, name, sport, SUM(score) AS total_score, SUM(GOLD) AS GOLD, SUM(SILVER) AS SILVER, SUM(BRONZE) AS BRONZE
    FROM athletes_join_medals
    GROUP BY id, name, sport
    ORDER BY total_score DESC, GOLD DESC, SILVER DESC, BRONZE DESC, name ASC
''')

athletes_join_medals.createOrReplaceTempView('athletes_join_medals')

athletes_join_medals = spark.sql('''
    SELECT id, name, sport, total_score, GOLD, SILVER, BRONZE,
    ROW_NUMBER() OVER (PARTITION BY sport ORDER BY total_score DESC, GOLD DESC, SILVER DESC, BRONZE DESC, name ASC) AS rank
    FROM athletes_join_medals
    ORDER BY sport ASC, total_score DESC, GOLD DESC, SILVER DESC, BRONZE DESC, name ASC
''')

athletes_join_medals.createOrReplaceTempView('athletes_join_medals')

athletes_join_medals = spark.sql('''
    SELECT id, name, sport, total_score, GOLD, SILVER, BRONZE, rank
    FROM athletes_join_medals
    WHERE rank = 1
    ORDER BY sport ASC, total_score DESC, GOLD DESC, SILVER DESC, BRONZE DESC, name ASC
''')

athletes_join_medals.createOrReplaceTempView('athletes_join_medals')

# athletes_join_medals.show()

result_task_1 = athletes_join_medals

# Task 1.2

athletes_join_medals.unpersist()

athletes_join_medals = spark.sql('''
    SELECT a.id AS id, a.name AS name, a.sport AS sport, a.country as athlete_country, m.year AS year, m.medal AS medal, c.id AS coach_id, c.name AS coach_name,
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
    FROM athletes a, medals m, coaches c
    WHERE a.id = m.id AND a.sport = m.sport AND a.coach_id = c.id AND a.sport = c.sport AND a.country IN ('USA', 'CHINA', 'INDIA') AND a.event = m.event AND a.year = m.year
''')

athletes_join_medals.createOrReplaceTempView('athletes_join_medals')

athletes_coaches_china = spark.sql('''
    SELECT * FROM athletes_join_medals
    WHERE athlete_country = 'CHINA'
''')

athletes_coaches_china.createOrReplaceTempView('athletes_coaches_china')

athletes_coaches_india = spark.sql('''
    SELECT * FROM athletes_join_medals
    WHERE athlete_country = 'INDIA'
''')

athletes_coaches_india.createOrReplaceTempView('athletes_coaches_india')

athletes_coaches_usa = spark.sql('''
    SELECT * FROM athletes_join_medals
    WHERE athlete_country = 'USA'
''')

athletes_coaches_usa.createOrReplaceTempView('athletes_coaches_usa')

top_5_coaches_china = spark.sql('''
    SELECT coach_id, coach_name, SUM(score) AS total_score, SUM(GOLD) AS GOLD, SUM(SILVER) AS SILVER, SUM(BRONZE) AS BRONZE
    FROM athletes_coaches_china
    GROUP BY coach_id, coach_name
    ORDER BY total_score DESC, GOLD DESC, SILVER DESC, BRONZE DESC, coach_name ASC
    LIMIT 5
''')

top_5_coaches_china.createOrReplaceTempView('top_5_coaches_china')

# top_5_coaches_china.show()

top_5_coaches_india = spark.sql('''
    SELECT coach_id, coach_name, SUM(score) AS total_score, SUM(GOLD) AS GOLD, SUM(SILVER) AS SILVER, SUM(BRONZE) AS BRONZE
    FROM athletes_coaches_india
    GROUP BY coach_id, coach_name
    ORDER BY total_score DESC, GOLD DESC, SILVER DESC, BRONZE DESC, coach_name ASC
    LIMIT 5
''')

top_5_coaches_india.createOrReplaceTempView('top_5_coaches_india')

# top_5_coaches_india.show()

top_5_coaches_usa = spark.sql('''
    SELECT coach_id, coach_name, SUM(score) AS total_score, SUM(GOLD) AS GOLD, SUM(SILVER) AS SILVER, SUM(BRONZE) AS BRONZE
    FROM athletes_coaches_usa
    GROUP BY coach_id, coach_name
    ORDER BY total_score DESC, GOLD DESC, SILVER DESC, BRONZE DESC, coach_name ASC
    LIMIT 5
''')

top_5_coaches_usa.createOrReplaceTempView('top_5_coaches_usa')

# top_5_coaches_usa.show()

result_task_1 = [row.name for row in result_task_1.collect()]

result_task_2 = [row.coach_name for row in top_5_coaches_china.collect()] + [row.coach_name for row in top_5_coaches_india.collect()] + [row.coach_name for row in top_5_coaches_usa.collect()]

with open(output_file, 'w') as f:
    result = (result_task_1, result_task_2)
    f.write(str(result) + "\n")

spark.stop()
