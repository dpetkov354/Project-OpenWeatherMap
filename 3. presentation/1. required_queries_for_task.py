# Databricks notebook source
# MAGIC %md
# MAGIC  
# MAGIC 2. Data model must contain data in order to get:
# MAGIC
# MAGIC       2.1 Distinct values of conditions (rain/snow/clear/…) for a given period;
# MAGIC
# MAGIC       2.2 Most common weather conditions in a certain period of time per city;
# MAGIC
# MAGIC       2.3 Temperature averages observed in a certain period per city;
# MAGIC
# MAGIC       2.4 City that had the highest absolute temperature in a certain period of time;
# MAGIC
# MAGIC       2.5 City that had the highest daily temperature variation in a certain period of time;
# MAGIC
# MAGIC       2.6 City that had the strongest wing in a certain period of time.
# MAGIC

# COMMAND ----------

# Database connection info
db_password = dbutils.secrets.get(scope="openweatherappdl-scope", key="db-password")

jdbc_url = "jdbc:postgresql://openweatherapp.postgres.database.azure.com:5432/openweatherappdb"

properties = {
    "user": "dpetkov354",
    "password": db_password,
    "driver": "org.postgresql.Driver"
}

# COMMAND ----------

#2.1 Distinct values of conditions (rain/snow/clear/…) for a given period;
# Define the SQL query
query = """
            (SELECT DISTINCT w.weather_main
            FROM measurement m
            JOIN weather w ON m.weather_id = w.weather_id
            WHERE m.dt BETWEEN '2024-03-18 10:10:10' AND '2024-03-26 10:10:10') AS subquery
        """

# Execute the query
df = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)

# Show the result
df.show()



# COMMAND ----------

#2.2 Most common weather conditions in a certain period of time per city;
#Ranking as number of conditions is not defined. First filtering by date then joining to reduce data movement
query = """
          (SELECT m.city_id, c.city_name, w.weather_main, COUNT(*) AS frequency
          FROM (          
                SELECT *
                FROM measurement
                WHERE dt BETWEEN '2024-03-18 10:10:10' AND '2024-03-26 10:10:10') m
          JOIN city c ON m.city_id = c.city_id
          JOIN weather w ON m.weather_id = w.weather_id
          GROUP BY m.city_id, c.city_name, w.weather_main
          ORDER BY frequency DESC)  AS subquery
        """

# Execute the query
df = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)

# Show the result
df.show()

# COMMAND ----------

#2.2 Most common weather conditions in a certain period of time per city;
#Ranking the highest accuring condition
query = """
          (SELECT m.city_id, c.city_name, w.weather_main, COUNT(*) AS frequency
          FROM (          
                SELECT *
                FROM measurement
                WHERE dt BETWEEN '2024-03-18 10:10:10' AND '2024-03-26 10:10:10') m
          JOIN city c ON m.city_id = c.city_id
          JOIN weather w ON m.weather_id = w.weather_id
          GROUP BY m.city_id, c.city_name, w.weather_main
          ORDER BY frequency DESC
          LIMIT 1)  AS subquery
        """

# Execute the query
df = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)

# Show the result
df.show()

# COMMAND ----------

#2.3 Temperature averages observed in a certain period per city
#Also rouding to the second decimal space
query = """
            (SELECT m.city_id, c.city_name, CAST(AVG(m.temperature) AS DECIMAL(10,2)) AS avg_temperature
            FROM (
                    SELECT *
                    FROM measurement
                    WHERE dt BETWEEN '2024-03-18 10:10:10' AND '2024-03-26 10:10:10'
                    ) m
            JOIN city c ON m.city_id = c.city_id
            GROUP BY m.city_id, c.city_name) AS subquery
        """

# Execute the query
df = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)

# Show the result
df.show()


# COMMAND ----------

#2.4 City that had the highest absolute temperature in a certain period of time
query = """
            (SELECT m.city_id, c.city_name, CAST(MAX(m.temp_max) AS DECIMAL(10,2)) AS max_temperature
            FROM (
                  SELECT *
                  FROM measurement
                  WHERE dt BETWEEN '2024-03-18 10:10:10' AND '2024-03-26 10:10:10'
                  ) m
            JOIN city c ON m.city_id = c.city_id
            GROUP BY m.city_id, c.city_name
            ORDER BY max_temperature DESC
            LIMIT 1) as subquery
        """
# Execute the query
df = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)

# Show the result
df.show()

# COMMAND ----------

#2.5 City that had the highest daily temperature variation in a certain period of time;
# The column measurement_id is not required, but it is useful for testing
query = """
            (SELECT m.measurement_id, m.city_id, c.city_name, 
                                            (MAX(m.temp_max) - MIN(m.temp_min)) AS temperature_variation
            FROM (
                    SELECT *
                    FROM measurement
                    WHERE dt BETWEEN '2024-03-18 10:10:10' AND '2024-03-26 10:10:10'
                ) m
            JOIN city c ON m.city_id = c.city_id
            WHERE m.dt BETWEEN m.sys_sunrise AND m.sys_sunset
            GROUP BY m.measurement_id, m.city_id, c.city_name
            ORDER BY temperature_variation DESC
            LIMIT 1) as subquery
        """
# Execute the query
df = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)

# Show the result
df.show()

# COMMAND ----------

#2.6 City that had the strongest wing in a certain period of time.
# The column measurement_id is not required, but it is useful for testing
query = """
            (SELECT m.measurement_id, m.city_id, c.city_name, 
                                            CAST(MAX(m.wind_speed) AS DECIMAL(10,2)) AS max_wind_speed
            FROM (
                    SELECT *
                    FROM measurement
                    WHERE dt BETWEEN '2024-03-18 10:10:10' AND '2024-03-26 10:10:10'
            ) m
            JOIN city c ON m.city_id = c.city_id
            GROUP BY m.measurement_id, m.city_id, c.city_name
            ORDER BY max_wind_speed DESC
            LIMIT 1) as subquery
        """
# Execute the query
df = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)

# Show the result
df.show()
