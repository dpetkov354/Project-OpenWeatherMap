# Databricks notebook source
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Data Processing") \
    .getOrCreate()

# Database connection info
db_password = dbutils.secrets.get(scope="openweatherappdl-scope", key="db-password")

jdbc_url = "jdbc:postgresql://openweatherapp.postgres.database.azure.com:5432/openweatherappdb"

properties = {
    "user": "dpetkov354",
    "password": db_password,
    "driver": "org.postgresql.Driver"
}


# COMMAND ----------


# Read data from PostgreSQL raw table
raw_df = spark.read.jdbc(url=jdbc_url, table="(SELECT DISTINCT weather_id, weather_main, weather_description, weather_icon FROM raw) AS raw_distinct", properties=properties)
raw_df = raw_df.alias("raw")
display(raw_df)


# COMMAND ----------

# Read data from PostgreSQL city table
weather_df = spark.read.jdbc(url=jdbc_url, table="weather", properties=properties)
weather_df = weather_df.alias("weather")
display(weather_df)

# COMMAND ----------

# Perform a left join to identify new distinct rows in raw_df
new_rows_df = raw_df.join(weather_df, 
                          (raw_df.weather_id == weather_df.weather_id) &
                          (raw_df.weather_main == weather_df.weather_main) &
                          (raw_df.weather_description == weather_df.weather_description) &
                          (raw_df.weather_icon == weather_df.weather_icon),
                          "left_outer") \
                   .filter(weather_df.weather_id.isNull())

display(new_rows_df)


# COMMAND ----------

#Leave only rows with NULL, so we have only the new rows. The aliases where needed for this part
new_distinct_rows_df = new_rows_df.select("raw.weather_id", "raw.weather_main", "raw.weather_description", "raw.weather_icon") \
                                .filter("weather.weather_id IS NULL")
display(new_distinct_rows_df)

# COMMAND ----------

#Append the city table
new_distinct_rows_df.write.jdbc(url=jdbc_url, table="weather", mode="append", properties=properties)
