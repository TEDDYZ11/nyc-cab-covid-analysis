#This script combines all parquet outputs from spark

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Combine-Parquet").getOrCreate()

df = spark.read.parquet("processed_nyc_taxi.parquet/")
df.coalesce(1).write.mode("overwrite").parquet("full_join.parquet")
