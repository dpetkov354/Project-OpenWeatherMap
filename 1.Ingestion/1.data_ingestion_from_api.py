# Databricks notebook source
import requests
import datetime
import pytz


#Unix time to timestamp to GMT+0200 (Eastern European Standard Time)
def dt_to_timestamp(api_dt):

    # Convert Unix timestamp to datetime object in UTC
    utc_datetime = datetime.datetime.utcfromtimestamp(api_dt)
    # Define timezone with offset (e.g., GMT+2)
    timezone_offset = datetime.timedelta(hours=2)  # Adjust offset as needed
    timezone = pytz.FixedOffset(timezone_offset.total_seconds() // 60)
    # Convert UTC datetime to local datetime with timezone offset
    local_datetime = utc_datetime.replace(tzinfo=pytz.utc).astimezone(timezone)
    # Format the local datetime object as a string
    formatted_timestamp = local_datetime.strftime('%a %b %d %Y %H:%M:%S GMT%z')
    return formatted_timestamp

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

# OpenWeatherMap API key
api_key = "67b8af2a44ee02263e53f9863816ff8c"

# List of cities for which you want to retrieve weather data
cities = ["Milano", "Bologna", "Cagliari"]

# Units of measurement
units = "metric"

# Weather data for each city
weather_data = {}

# Retrieve weather data for each city
for city in cities:
    data = get_weather_data(api_key, city, units)
    if data:
        weather_data[city] = data
        # Extract relevant information
        city_id = data['id']
        city_name = data['name']
        #longitude = data['coord']['lon']
        #latitude = data['coord']['lat']
        weather_id = data['weather'][0]['id']
        weather_main = data['weather'][0]['main']
        weather_description = data['weather'][0]['description']
        #weather_icon = data['weather'][0]['icon']
        #base = data['base']
        temperature = data['main']['temp']
        #feels_like = data['main']['feels_like']
        temp_min = data['main']['temp_min']
        temp_max = data['main']['temp_max']
        #pressure = data['main']['pressure']
        #humidity = data['main']['humidity']
        #visibility = data['visibility']
        wind_speed = data['wind']['speed']
        #wind_deg = data['wind']['deg']
        #clouds_all = data['clouds']['all']
        dt = dt_to_timestamp(data['dt'])
        sys_type = data['sys']['type']
        sys_id = data['sys']['id']
        sys_country = data['sys']['country']
        sys_sunrise = data['sys']['sunrise']
        sys_sunset = data['sys']['sunset']
        timezone = data['timezone']
        cod = data['cod']

        # Create DataFrame
        data = [(city_id,
                 city_name, 
                 #longitude, 
                 #latitude, 
                 weather_id, 
                 weather_main, 
                 weather_description, 
                 #weather_icon, 
                 #base, 
                 temperature, 
                 #feels_like, 
                 temp_min, 
                 temp_max, 
                 #pressure, 
                 #humidity, 
                 #visibility, 
                 wind_speed, 
                 #wind_deg, 
                 #clouds_all, 
                 dt, 
                 sys_type, 
                 sys_id, 
                 sys_country, 
                 sys_sunrise, 
                 sys_sunset, 
                 timezone, 
                 cod)]
        
        columns = ["city_id", 
                   "city_name", 
                   #"longitude", 
                   #"latitude", 
                   "weather_id", 
                   "weather_main", 
                   "weather_description", 
                   #"weather_icon", 
                   #"base", 
                   "temperature", 
                   #"feels_like", 
                   "temp_min", 
                   "temp_max", 
                   #"pressure", 
                   #"humidity", 
                   #"visibility", 
                   "wind_speed", 
                   #"wind_deg", 
                   #"clouds_all", 
                   "dt", 
                   "sys_type", 
                   "sys_id", 
                   "sys_country", 
                   "sys_sunrise", 
                   "sys_sunset", 
                   "timezone", 
                   "cod"]
        df = spark.createDataFrame(data, columns)

        # Show DataFrame
        display(df)
        
    else:
        print("Failed to retrieve weather data for", city)



