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
raw_df = spark.read.jdbc(url=jdbc_url, table="raw", properties=properties)
raw_df = raw_df.alias("raw")
# display(raw_df)


# COMMAND ----------

# Read data from PostgreSQL measurement table
measurement_df = spark.read.jdbc(url=jdbc_url, table="measurement", properties=properties)
measurement_df = measurement_df.alias("measurement")
# display(measurement_df)

# COMMAND ----------

# Perform a left join to identify new distinct rows in raw_df
new_rows_df = raw_df.join(measurement_df, 
                          (raw_df.measurement_id == measurement_df.measurement_id),
                             "left_outer") \
                   .filter(measurement_df.measurement_id.isNull())

# display(new_rows_df)


# COMMAND ----------

#Leave only rows with NULL, so we have only the new rows. The aliases where needed for this part
new_distinct_rows_df = new_rows_df.select("raw.measurement_id", 
                                          "raw.city_id", 
                                          "raw.weather_id", 
                                          "raw.temperature", 
                                          "raw.temp_min", 
                                          "raw.temp_max", 
                                          "raw.wind_speed", 
                                          "raw.dt", 
                                          "raw.sys_country", 
                                          "raw.sys_sunrise", 
                                          "raw.sys_sunset", 
                                          "raw.timezone") \
                                .filter("measurement.measurement_id IS NULL")
# display(new_distinct_rows_df)

# COMMAND ----------

#Append the city table
new_distinct_rows_df.write.jdbc(url=jdbc_url, table="measurement", mode="append", properties=properties)
