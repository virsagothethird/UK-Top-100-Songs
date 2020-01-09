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

df = sqlContext.read.csv("uk100.csv", header=True)