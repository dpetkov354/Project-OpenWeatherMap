# Databricks notebook source
# PostgreSQL connection properties
db_password = dbutils.secrets.get(scope="openweatherappdl-scope", key="db-password")
jdbc_url = "jdbc:postgresql://openweatherapp.postgres.database.azure.com:5432/openweatherappdb"
properties = {
    "user": "dpetkov354",
    "password": db_password,
    "driver": "org.postgresql.Driver"
}

table_name = "temp_view"


# COMMAND ----------


#1.Save the raw data to a csv in DBFS

# Create a temporary view for the PostgreSQL table
spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "raw") \
    .option("user", "dpetkov354") \
    .option("password", db_password) \
    .option("driver", "org.postgresql.Driver") \
    .load() \
    .createOrReplaceTempView(table_name)

# Read data from the temporary view into a DataFrame using SQL query
query = """
        (SELECT * FROM raw)
        """
df = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)

# Define the DBFS path where you want to save the CSV file
dbfs_csv_file_path = "dbfs:/user/hive/warehouse/openweaterapp.db/raw.csv"

# Write DataFrame to a CSV file directly to the specified DBFS path
df.write.csv(dbfs_csv_file_path, header=True, mode="overwrite")

print("CSV file has been saved successfully to DBFS:", dbfs_csv_file_path)

# COMMAND ----------

#2.Save the city data to a csv in DBFS

spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "city") \
    .option("user", "dpetkov354") \
    .option("password", db_password) \
    .option("driver", "org.postgresql.Driver") \
    .load() \
    .createOrReplaceTempView(table_name)

query = """
        (SELECT * FROM city)
        """
df = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)

dbfs_csv_file_path = "dbfs:/user/hive/warehouse/openweaterapp.db/city.csv"

df.write.csv(dbfs_csv_file_path, header=True, mode="overwrite")

print("CSV file has been saved successfully to DBFS:", dbfs_csv_file_path)

# COMMAND ----------

#3.Save the weather data to a csv in DBFS

spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "city") \
    .option("user", "dpetkov354") \
    .option("password", db_password) \
    .option("driver", "org.postgresql.Driver") \
    .load() \
    .createOrReplaceTempView(table_name)

query = """
        (SELECT * FROM weather)
        """
df = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)

dbfs_csv_file_path = "dbfs:/user/hive/warehouse/openweaterapp.db/weather.csv"

df.write.csv(dbfs_csv_file_path, header=True, mode="overwrite")

print("CSV file has been saved successfully to DBFS:", dbfs_csv_file_path)

# COMMAND ----------

#4.Save the measurements data to a csv in DBFS

spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "measurement") \
    .option("user", "dpetkov354") \
    .option("password", db_password) \
    .option("driver", "org.postgresql.Driver") \
    .load() \
    .createOrReplaceTempView(table_name)

query = """
        (SELECT * FROM measurement)
        """
df = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)

dbfs_csv_file_path = "dbfs:/user/hive/warehouse/openweaterapp.db/measurement.csv"

df.write.csv(dbfs_csv_file_path, header=True, mode="overwrite")

print("CSV file has been saved successfully to DBFS:", dbfs_csv_file_path)
