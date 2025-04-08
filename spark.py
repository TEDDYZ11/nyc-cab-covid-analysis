from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import DateType, IntegerType, FloatType

# have to limit the spark usage due to free tier
spark = SparkSession.builder \
    .appName("405") \
    .config("spark.master", "local[1]") \
    .config("spark.driver.memory", "384m") \
    .config("spark.executor.memory", "480m") \
    .config("spark.sql.shuffle.partitions", "1") \
    .config("spark.default.parallelism", "1") \
    .getOrCreate()


file_list = [
    f"./raw_data/yellow_taxi_data/yellow_tripdata_2020-0{i}.parquet" 
    for i in range(1, 6)
]

numeric_columns = [
    "CASE_COUNT", "PROBABLE_CASE_COUNT", "HOSPITALIZED_COUNT", "DEATH_COUNT",
    "CASE_COUNT_7DAY_AVG", "ALL_CASE_COUNT_7DAY_AVG", "HOSP_COUNT_7DAY_AVG", "DEATH_COUNT_7DAY_AVG",
    "BX_CASE_COUNT", "BX_PROBABLE_CASE_COUNT", "BX_HOSPITALIZED_COUNT", "BX_DEATH_COUNT",
    "BX_CASE_COUNT_7DAY_AVG", "BX_PROBABLE_CASE_COUNT_7DAY_AVG", "BX_ALL_CASE_COUNT_7DAY_AVG",
    "BX_HOSPITALIZED_COUNT_7DAY_AVG", "BX_DEATH_COUNT_7DAY_AVG",
    "BK_CASE_COUNT", "BK_PROBABLE_CASE_COUNT", "BK_HOSPITALIZED_COUNT", "BK_DEATH_COUNT",
    "BK_CASE_COUNT_7DAY_AVG", "BK_PROBABLE_CASE_COUNT_7DAY_AVG", "BK_ALL_CASE_COUNT_7DAY_AVG",
    "BK_HOSPITALIZED_COUNT_7DAY_AVG", "BK_DEATH_COUNT_7DAY_AVG",
    "MN_CASE_COUNT", "MN_PROBABLE_CASE_COUNT", "MN_HOSPITALIZED_COUNT", "MN_DEATH_COUNT",
    "MN_CASE_COUNT_7DAY_AVG", "MN_PROBABLE_CASE_COUNT_7DAY_AVG", "MN_ALL_CASE_COUNT_7DAY_AVG",
    "MN_HOSPITALIZED_COUNT_7DAY_AVG", "MN_DEATH_COUNT_7DAY_AVG",
    "QN_CASE_COUNT", "QN_PROBABLE_CASE_COUNT", "QN_HOSPITALIZED_COUNT", "QN_DEATH_COUNT",
    "QN_CASE_COUNT_7DAY_AVG", "QN_PROBABLE_CASE_COUNT_7DAY_AVG", "QN_ALL_CASE_COUNT_7DAY_AVG",
    "QN_HOSPITALIZED_COUNT_7DAY_AVG", "QN_DEATH_COUNT_7DAY_AVG",
    "SI_CASE_COUNT", "SI_PROBABLE_CASE_COUNT", "SI_HOSPITALIZED_COUNT", "SI_DEATH_COUNT",
    "SI_CASE_COUNT_7DAY_AVG", "SI_PROBABLE_CASE_COUNT_7DAY_AVG", "SI_ALL_CASE_COUNT_7DAY_AVG",
    "SI_HOSPITALIZED_COUNT_7DAY_AVG", "SI_DEATH_COUNT_7DAY_AVG", "INCOMPLETE"
]

# Load NYC Taxi data
df_taxi = spark.read.parquet(*file_list)
df_covid = spark.read.option("delimiter", ",").option("header", True).csv("./raw_data/covid_data/data_by_day.csv")
df_zones = spark.read.option("delimiter", ",").option("header", True).csv("./raw_data/taxi_zone_lookup.csv")

#deleting invalid trips
df_taxi = df_taxi.filter(col("fare_amount") > 0)

#making sure the join keys are date time
df_taxi = df_taxi.withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
df_covid = df_covid.withColumn("date", to_date(col("date_of_interest"), "MM/dd/yyyy"))

#filtering out dates 
df_taxi = df_taxi.filter((col("pickup_date") >= "2020-01-01") & (col("pickup_date") <= "2020-05-31"))

for column in numeric_columns:
    df_covid = df_covid.withColumn(column, col(column).cast(FloatType()))


df_locations_pickup = df_zones.alias("pickup")
df_locations_dropoff = df_zones.alias("dropoff")
# Join datasets
df_joined = df_taxi.join(df_covid, df_taxi.pickup_date == df_covid.date, "left") \
		   .join(df_locations_pickup, df_taxi.PULocationID == df_locations_pickup.LocationID, "left") \
		   .join(df_locations_dropoff, df_taxi.DOLocationID == df_locations_dropoff.LocationID, "left") \
		   .select(
        df_taxi	["*"],
	    df_covid["*"],
        col("pickup.Borough").alias("Pickup_Borough"),
        col("pickup.Zone").alias("Pickup_Zone"),
        col("dropoff.Borough").alias("Dropoff_Borough"),
        col("dropoff.Zone").alias("Dropoff_Zone")
    )

# Save data
df_joined.write.mode("overwrite").parquet("processed_nyc_taxi.parquet")

#pre aggregate daily trip counts by pickup zone
df_daily_trips = df_joined.groupBy("pickup_date", "Pickup_Zone", "Dropoff_Zone") \
                        .count() \
                        .withColumnRenamed("count", "trip_count")

df_daily_trips.write.mode("overwrite").parquet("daily_trips_zone.parquet")

#pre aggregate daily trip by borough
df_daily_trips = df_joined.groupBy("pickup_date", "Pickup_Borough", "Dropoff_Borough") \
                        .count() \
                        .withColumnRenamed("count", "trip_count")

df_daily_trips.write.mode("overwrite").parquet("daily_trips_borough.parquet")
