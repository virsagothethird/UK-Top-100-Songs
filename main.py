import pyspark as ps
import pyspark.sql.functions as f
from pyspark import SQLContext
from pyspark.sql.types import IntegerType, DateType, TimestampType
from datetime import datetime

import matplotlib.pyplot as plt
import scipy.stats as stats

spark = (ps.sql.SparkSession.builder 
        .master("local[4]") 
        .appName("sparkSQL exercise") 
        .getOrCreate()
        )
sc = spark.sparkContext
sqlContext = SQLContext(sc)



# Importing csv into spark df
df = sqlContext.read.csv("uk100.csv", header=True)

# Drop unneeded columns
df = df.drop('_c0')
df = df.drop('hmm')

# lowercase all values for simpler eda
for col in df.columns:
    df = df.withColumn(col, f.lower(f.col(col)))

# Cast columns to their appropriate types
df = df.withColumn("rank", df["rank"].cast(IntegerType()))
df = df.withColumn("peak_rank", df["peak_rank"].cast(IntegerType()))
df = df.withColumn("weeks_on_chart", df["weeks_on_chart"].cast(IntegerType()))

# Change the date column to the datetype
def to_date(x):
    return datetime.strptime(x, '%B %d %Y')
change_to_datetype = f.udf(lambda y: to_date(y), DateType())

df = df.withColumn("week_of", change_to_datetype('week_of'))

# Create temp table for eda
df.createOrReplaceTempView('test')

# Create new df that has filtered out songs released before 2015
new_songs = spark.sql('''
            SELECT artist, title
            FROM test
            WHERE last_week_rank like "%new%"''')
new_songs_2015_2019 = df.join(new_songs, ['title', 'artist'],how='leftsemi')

# Filter out songs that have peaked below a rank of 75
peak_75 = spark.sql('''
            SELECT artist, title, MIN(peak_rank) as real_peak
            FROM test
            GROUP BY artist, title
            HAVING real_peak <= 75
            ''')
songs_2015_2019 = new_songs_2015_2019.join(peak_75, ['title', 'artist'],how='leftsemi')
songs_2015_2019.createOrReplaceTempView('test2')

# More filtering and cleaning
final_df = spark.sql('''
            SELECT artist, title, COUNT(*) AS num_weeks, ROUND(AVG(rank),2) AS avg_rank, MIN(week_of) AS entrance_date
            FROM test2
            GROUP BY 1,2
            ORDER BY 5''')
final_df.createOrReplaceTempView('test3')

# Plotting lifespan counts
lifespan_count = spark.sql('''
            SELECT *
            FROM test3
            ''')
lifespan_count = lifespan_count.rdd.map(lambda row: row.num_weeks).collect()
fig, ax = plt.subplots(figsize=(10,6))
ax.hist(lifespan, bins=50)
ax.set_xlim(0, 120)
ax.set_ylim(0, 600)
ax.set_title("Lifespan Counts", fontsize=20)
ax.set_ylabel("Number of Songs", fontsize=12)
ax.set_xlabel("Lifespan(weeks)", fontsize=12)
ax.grid(alpha=0.15)



num_per_month_test = spark.sql('''
            SELECT MONTH(entrance_date) as month, COUNT(*) as count
            FROM test3
            GROUP BY MONTH(entrance_date)
            ORDER BY 1
            ''').collect()