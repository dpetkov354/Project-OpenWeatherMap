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
raw_df = spark.read.jdbc(url=jdbc_url, table="(SELECT DISTINCT city_id, city_name, longitude, latitude FROM raw) AS raw_distinct", properties=properties)
raw_df = raw_df.alias("raw")
# display(raw_df)


# COMMAND ----------

# Read data from PostgreSQL city table
city_df = spark.read.jdbc(url=jdbc_url, table="city", properties=properties)
city_df = city_df.alias("city")
# display(city_df)

# COMMAND ----------

# Perform a left join to identify new distinct rows in raw_df
new_rows_df = raw_df.join(city_df, 
                          (raw_df.city_id == city_df.city_id) &
                          (raw_df.city_name == city_df.city_name) &
                          (raw_df.longitude == city_df.longitude) &
                          (raw_df.latitude == city_df.latitude),
                          "left_outer") \
                   .filter(city_df.city_id.isNull())

# display(new_rows_df)


# COMMAND ----------

#Leave only rows with NULL, so we have only the new rows
new_distinct_rows_df = new_rows_df.select("raw.city_id", "raw.city_name", "raw.longitude", "raw.latitude") \
                                .filter("city.city_id IS NULL")
#display(new_distinct_rows_df)

# COMMAND ----------

#Append the city table
new_distinct_rows_df.write.jdbc(url=jdbc_url, table="city", mode="append", properties=properties)
