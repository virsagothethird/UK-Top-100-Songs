import pyspark as ps
import pyspark.sql.functions as f
from pyspark import SQLContext
from pyspark.sql.types import IntegerType, DateType, TimestampType
from datetime import datetime
import matplotlib.pyplot as plt
import scipy.stats as stats
import numpy as np


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
plt.savefig("img/Lifespan_counts.png")



# Plotting average number of songs released per month
num_per_month = spark.sql('''
            SELECT MONTH(entrance_date) as month, COUNT(*) as num_songs
            FROM test3
            WHERE entrance_date < "2019-01-01"
            GROUP BY MONTH(entrance_date)
            ORDER BY 1
            ''')
num_per_month = num_per_month.rdd.map(lambda row: row.num_songs).collect()

months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']

fig, ax = plt.subplots(figsize=(12,6))
ax.barh(months, num_per_month, height=0.5)
ax.invert_yaxis()
for i, v in enumerate(num_per_month):
    ax.text(v + 3, i + .25, str(v), color='blue')
ax.set_xlim(0,200)
ax.set_title('Number of Songs Per Month (removed: Songs released in 2019)')
ax.set_xlabel("Number of Songs", fontsize=12)
ax.grid(alpha=0.15)
plt.savefig("img/num_songs_month.png")



# Plotting average lifespan per month (removed: songs released in 2019)
avg_lifespan_per_month = spark.sql('''
            SELECT MONTH(entrance_date) AS month, ROUND(AVG(num_weeks),2) AS avg_lifespan
            FROM test3
            WHERE entrance_date < "2019-01-01"
            GROUP BY MONTH(entrance_date)
            ORDER BY 1''')
avg_lifespan = avg_lifespan_per_month.rdd.map(lambda row: row.avg_lifespan).collect()

fig, ax = plt.subplots(figsize=(12,6))
ax.barh(months, avg_lifespan, height=0.5)
ax.invert_yaxis()
for i, v in enumerate(avg_lifespan):
    ax.text(v + 0.25, i + .1, str(v), color='blue')
ax.axvline(13.43, color='r', linestyle="--", alpha=0.4, label="Overall Average Lifespan")
ax.set_xlim(0,21)
ax.set_title('Average Lifespan (Without songs released in 2019)')
ax.set_xlabel("Lifespan (weeks)", fontsize=12)
ax.legend()
plt.savefig("img/avg_lifespan_month.png")



# Begin sampling data
def samp(month):
    sample = spark.sql('''
                SELECT num_weeks
                FROM test3
                WHERE MONTH(entrance_date)={}
                    AND entrance_date < "2019-01-01"'''.format(month))
    return sample.rdd.map(lambda row: row.num_weeks).collect()

def samp_exclude_month(month):
    sample = spark.sql('''
                SELECT num_weeks
                FROM test3
                WHERE entrance_date < "2019-01-01"
                    AND MONTH(entrance_date) <> {}'''.format(month))
    return sample.rdd.map(lambda row: row.num_weeks).collect()

def bootstrap(x, resamples=1000):
    bootstrap_samples=[]
    for i in range(resamples):
        bootstrap = np.random.choice(x, size=30, replace=True)
        bootstrap_samples.append(np.mean(bootstrap))
    return bootstrap_samples

jan = samp(1)
not_jan = samp_exclude_month(1)
feb = samp(2)
not_feb = samp_exclude_month(2)
mar = samp(3)
not_mar = samp_exclude_month(3)
apr = samp(4)
not_apr = samp_exclude_month(4)
may = samp(5)
not_may = samp_exclude_month(5)
jun = samp(6)
not_jun = samp_exclude_month(6)
jul = samp(7)
not_jul = samp_exclude_month(7)
aug = samp(8)
not_aug = samp_exclude_month(8)
sep = samp(9)
not_sep = samp_exclude_month(9)
octo = samp(10)
not_octo = samp_exclude_month(10)
nov = samp(11)
not_nov = samp_exclude_month(11)
dec = samp(12)
not_dec = samp_exclude_month(12)

jan = bootstrap(jan)
feb = bootstrap(feb)
mar = bootstrap(mar)
apr = bootstrap(apr)
may = bootstrap(may)
jun = bootstrap(jun)
jul = bootstrap(jul)
aug = bootstrap(aug)
sep = bootstrap(sep)
octo = bootstrap(octo)
nov = bootstrap(nov)
dec = bootstrap(dec)

not_jan = bootstrap(not_jan)
not_feb = bootstrap(not_feb)
not_mar = bootstrap(not_mar)
not_apr = bootstrap(not_apr)
not_may = bootstrap(not_may)
not_jun = bootstrap(not_jun)
not_jul = bootstrap(not_jul)
not_aug = bootstrap(not_aug)
not_sep = bootstrap(not_sep)
not_octo = bootstrap(not_octo)
not_nov = bootstrap(not_nov)
not_dec = bootstrap(not_dec)

fig, ax = plt.subplots()
ax.hist(jan, alpha=.4, density=True, label="January")
ax.hist(feb, alpha=.4, density=True, label="February")
ax.hist(mar, alpha=.4, density=True, label="March")
ax.hist(apr, alpha=.4, density=True, label="April")
ax.set_title('First four months')
ax.set_xlabel("Lifespan (weeks)", fontsize=12)
ax.legend()
ax.grid(alpha=0.15)
plt.savefig("img/first_four_months.png")



# Multiple T-tests (Month vs the rest)
_, p1 = stats.ttest_ind(jan,not_jan)
_, p2 = stats.ttest_ind(feb,not_feb)
_, p3 = stats.ttest_ind(mar,not_mar)
_, p4 = stats.ttest_ind(apr,not_apr)
_, p5 = stats.ttest_ind(may,not_may)
_, p6 = stats.ttest_ind(jun,not_jun)
_, p7 = stats.ttest_ind(jul,not_jul)
_, p8 = stats.ttest_ind(aug,not_aug)
_, p9 = stats.ttest_ind(sep,not_sep)
_, p10 = stats.ttest_ind(octo,not_octo)
_, p11 = stats.ttest_ind(nov,not_nov)
_, p12 = stats.ttest_ind(dec,not_dec)

alpha = 0.05/12
print("corrected alpha = {}".format(alpha))
print("Jan vs rest = "+ str(p1))
print("Feb vs rest = "+ str(p2))
print("Mar vs rest = "+ str(p3))
print("Apr vs rest = "+ str(p4))
print("May vs rest = "+str(p5))
print("Jun vs rest = "+str(p6))
print("Jul vs rest = "+str(p7))
print("Aug vs rest = "+str(p8))
print("Sep vs rest = "+str(p9))
print("Oct vs rest = "+str(p10))
print("Nov vs rest = "+str(p11))
print("Dec vs rest = "+str(p12))