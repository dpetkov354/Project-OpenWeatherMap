# Databricks notebook source
import requests
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType, TimestampType
from datetime import datetime

# OpenWeatherMap API key
api_key = dbutils.secrets.get(scope="openweatherappdl-scope", key="openweather-api-key")

# List of cities for which you want to retrieve weather data
cities = ["Milano", "Bologna", "Cagliari"]

# Units of measurement
units = "metric"

# Weather data for each city
weather_data = {}

#Request function
def get_weather_data(api_key, city, units):
    base_url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": city,
        "appid": api_key,
        "units": units
    }
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to fetch weather data. Status code:", response.status_code)
        return None

# PostgreSQL connection properties
db_password = dbutils.secrets.get(scope="openweatherappdl-scope", key="db-password")
jdbc_url = "jdbc:postgresql://openweatherapp.postgres.database.azure.com:5432/openweatherappdb"
properties = {
    "user": "dpetkov354",
    "password": db_password,
    "driver": "org.postgresql.Driver"
}

schema = StructType([
    StructField("city_id", IntegerType(), True),
    StructField("city_name", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("weather_id", IntegerType(), True),
    StructField("weather_main", StringType(), True),
    StructField("weather_description", StringType(), True),
    StructField("weather_icon", StringType(), True),
    StructField("base", StringType(), True),
    StructField("temperature", FloatType(), True),
    StructField("feels_like", FloatType(), True),
    StructField("temp_min", FloatType(), True),
    StructField("temp_max", FloatType(), True),
    StructField("pressure", IntegerType(), True),
    StructField("humidity", IntegerType(), True),
    StructField("visibility", IntegerType(), True),
    StructField("wind_speed", FloatType(), True),
    StructField("wind_deg", IntegerType(), True),
    StructField("clouds_all", IntegerType(), True),
    StructField("dt", TimestampType(), True),
    StructField("sys_type", IntegerType(), True),
    StructField("sys_id", IntegerType(), True),
    StructField("sys_country", StringType(), True),
    StructField("sys_sunrise", TimestampType(), True),
    StructField("sys_sunset", TimestampType(), True),
    StructField("timezone", IntegerType(), True),
    StructField("cod", StringType(), True)
])

# COMMAND ----------

# Retrieve weather data for each city
for city in cities:
    data = get_weather_data(api_key, city, units)
    if data:
        # Extract information
        city_id = data['id']
        city_name = data['name']
        longitude = data['coord']['lon']
        latitude = data['coord']['lat']
        weather_id = data['weather'][0]['id']
        weather_main = data['weather'][0]['main']
        weather_description = data['weather'][0]['description']
        weather_icon = data['weather'][0]['icon']
        base = data['base']
        temperature = data['main']['temp']
        feels_like = data['main']['feels_like']
        temp_min = data['main']['temp_min']
        temp_max = data['main']['temp_max']
        pressure = data['main']['pressure']
        humidity = data['main']['humidity']
        visibility = data['visibility']
        wind_speed = data['wind']['speed']
        wind_deg = data['wind']['deg']
        clouds_all = data['clouds']['all']
        dt = datetime.utcfromtimestamp(data['dt'])
        sys_type = data['sys']['type']
        sys_id = data['sys']['id']
        sys_country = data['sys']['country']
        sys_sunrise = datetime.utcfromtimestamp(data['sys']['sunrise'])
        sys_sunset = datetime.utcfromtimestamp(data['sys']['sunset'])
        timezone = data['timezone']
        cod = data['cod']

        # Create DataFrame
        data = [(city_id,
                 city_name, 
                 longitude, 
                 latitude, 
                 weather_id, 
                 weather_main, 
                 weather_description, 
                 weather_icon, 
                 base, 
                 temperature, 
                 feels_like, 
                 temp_min, 
                 temp_max, 
                 pressure, 
                 humidity, 
                 visibility, 
                 wind_speed, 
                 wind_deg, 
                 clouds_all, 
                 dt, 
                 sys_type, 
                 sys_id, 
                 sys_country, 
                 sys_sunrise, 
                 sys_sunset, 
                 timezone, 
                 cod
                 )]
        
        display(data)

        df = df = spark.createDataFrame(data, schema)

        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "raw") \
            .option("user", properties["user"]) \
            .option("password", properties["password"]) \
            .option("driver", properties["driver"]) \
            .mode("append") \
            .save()

    else:
        print("Failed to retrieve weather data for", city)

# COMMAND ----------

df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "raw") \
    .option("user", properties["user"]) \
    .option("password", properties["password"]) \
    .option("driver", properties["driver"]) \
    .load()
display(df)
