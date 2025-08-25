# Databricks notebook source
from azure.eventhub import EventHubProducerClient, EventData
import json
# Event Hub Config
even_hub_connection_string = dbutils.secrets.get(scope = "key-vault-scope", key = "eventHubConnectionString")
EVENT_HUB_NAME = "hub-weather-streams"

# Create producer client
producer = EventHubProducerClient.from_connection_string(conn_str=EVENT_HUB_CONN_STRING, eventhub_name=EVENT_HUB_NAME)

# Function to send events to EventHub
def sendEvent(event):
    event_data_batch = producer.create_batch()
    event_data_batch.add(EventData(json.dumps(event)))
    producer.send_batch(event_data_batch)

# Sample event data
event = {
    "event_id": 9999,
    "event_name": "Key Vault Test_Event"
}

# Send event to EventHub
sendEvent(event)

producer.close()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##API Testing
# MAGIC

# COMMAND ----------

import json
import requests

# Handle Response
def handle_response(response):
    if response.status_code == 200:
        return response.json()
    else:
        return f"Error: {response.status_code}, {response.text}"

# Get Current Weather
def getCurrentWeather(baseURL, apiKey, location):
    currentWeather = f"{baseUrl}/current.json"
    params ={
    'key': weatherApiKey,
    'q': location,
    'aqi': 'yes'
    }
    response = requests.get(currentWeather, params=params)
    return handle_response(response)

# Get Forecast Weather
def getForecastWeather(baseURL, apiKey, location,days):
    forecastWeather = f"{baseUrl}/forecast.json"
    params ={
    'key': weatherApiKey,
    'q': location,
    'days': days
    }
    response = requests.get(forecastWeather, params=params)
    return handle_response(response)

# Get Alerts
def getAlertWeather(baseURL, apiKey, location):
    alertWeather = f"{baseUrl}/alerts.json"
    params ={
    'key': weatherApiKey,
    'q': location,
    'alerts': 'yes'
    }
    response = requests.get(alertWeather, params=params)
    return handle_response(response)

def flattenData(currentWeather, forecastWeather, alertWeather):
    CurrentLocation = currentWeather.get('location',{})
    current = currentWeather.get('current',{})
    condition = current.get('condition',{})
    airQuality = current.get('air_quality',{})
    forecast = forecastWeather.get('forecast', {}).get('forecastday', [])
    alert = alertWeather.get('alerts', {}).get('alert', [])

    flatten_Data = {
        'name':CurrentLocation.get('name'),
        'region':CurrentLocation.get('region'),
        'country':CurrentLocation.get('country'),
        'lat':CurrentLocation.get('lat'),
        'lon':CurrentLocation.get('lon'),
        'localtime':CurrentLocation.get('localtime'),
        'temp_c':current.get('temp_c'),
        'is_day':current.get('is_day'),
        'condition_text':condition.get('text'),
        'condition_icon':condition.get('icon'),
        'wind_kph':current.get('wind_kph'),
        'wind_degree':current.get('wind_degree'),
        'wind_dir':current.get('wind_dir'),
        'pressure_in':current.get('pressure_in'),
        'precip_in':current.get('precip_in'),
        'humidity':current.get('humidity'),
        'cloud':current.get('cloud'),
        'feelslike_c':current.get('feelslike_c'),
        'uv':current.get('uv'),
        'air-quality': {
            'co':airQuality.get('co'),
            'no2':airQuality.get('no2'),
            'o3':airQuality.get('o3'),
            'pm2_5':airQuality.get('pm2_5'),
            'pm10':airQuality.get('pm10'),
            'so2':airQuality.get('so2'),
            'us-epa-index':airQuality.get('us-epa-index'),
            'gb-defra-index':airQuality.get('gb-defra-index')
        },
        'alert':[
            {
                'headline':alert.get('headline'),
                'severity':alert.get('severity'),
                'desc':alert.get('desc'),
                'date':alert.get('date'),
                'category':alert.get('category'),
                'instructions':alert.get('instructions'),
            }
            for alert in alert
        ],
        'forecast':[
            {
                'date':forecast.get('date'),
                'maxtemp_c':forecast.get('day').get('maxtemp_c'),
                'mintemp_c':forecast.get('day'),
                'condition':forecast.get('day').get('condition').get('text'),
            }
            for forecast in forecast
        ]
    }
    return flatten_Data

def fetchData():

    # Getting secret value from key vault
    weatherApiKey = dbutils.secrets.get(scope = "key-vault-scope", key = "weatherapi")
    location = "Hyderabad"
    baseUrl = "http://api.weatherapi.com/v1/"

    currentWeather = getCurrentWeather(baseUrl, weatherApiKey, location)
    forecastWeather = getForecastWeather(baseUrl, weatherApiKey, location, 3)
    alertWeather = getAlertWeather(baseUrl, weatherApiKey, location)

    # print('Current Weather: \n', json.dumps(currentWeather, indent=3))
    # print('Forecast Weather: \n', json.dumps(forecastWeather, indent=3))
    # print('Alert Weather: \n', json.dumps(alertWeather, indent=3))
    mergedData = flattenData(currentWeather, forecastWeather, alertWeather)

    print('Weather Data: \n', json.dumps(mergedData, indent=3))

fetchData()

# COMMAND ----------

import json
import requests
from azure.eventhub import EventHubProducerClient, EventData

# Event Hub Config
even_hub_connection_string = dbutils.secrets.get(scope = "key-vault-scope", key = "eventHubConnectionString")
EVENT_HUB_NAME = "hub-weather-streams"

# Create producer client
producer = EventHubProducerClient.from_connection_string(conn_str=even_hub_connection_string, eventhub_name=EVENT_HUB_NAME)

# Function to send events to EventHub
def sendEvent(event):
    event_data_batch = producer.create_batch()
    event_data_batch.add(EventData(json.dumps(event)))
    producer.send_batch(event_data_batch)

# Handle Response
def handle_response(response):
    if response.status_code == 200:
        return response.json()
    else:
        return f"Error: {response.status_code}, {response.text}"

# Get Current Weather
def getCurrentWeather(baseURL, apiKey, location):
    currentWeather = f"{baseUrl}/current.json"
    params ={
    'key': weatherApiKey,
    'q': location,
    'aqi': 'yes'
    }
    response = requests.get(currentWeather, params=params)
    return handle_response(response)

# Get Forecast Weather
def getForecastWeather(baseURL, apiKey, location,days):
    forecastWeather = f"{baseUrl}/forecast.json"
    params ={
    'key': weatherApiKey,
    'q': location,
    'days': days
    }
    response = requests.get(forecastWeather, params=params)
    return handle_response(response)

# Get Alerts
def getAlertWeather(baseURL, apiKey, location):
    alertWeather = f"{baseUrl}/alerts.json"
    params ={
    'key': weatherApiKey,
    'q': location,
    'alerts': 'yes'
    }
    response = requests.get(alertWeather, params=params)
    return handle_response(response)

def flattenData(currentWeather, forecastWeather, alertWeather):
    CurrentLocation = currentWeather.get('location',{})
    current = currentWeather.get('current',{})
    condition = current.get('condition',{})
    airQuality = current.get('air_quality',{})
    forecast = forecastWeather.get('forecast', {}).get('forecastday', [])
    alert = alertWeather.get('alerts', {}).get('alert', [])

    flatten_Data = {
        'name':CurrentLocation.get('name'),
        'region':CurrentLocation.get('region'),
        'country':CurrentLocation.get('country'),
        'lat':CurrentLocation.get('lat'),
        'lon':CurrentLocation.get('lon'),
        'localtime':CurrentLocation.get('localtime'),
        'temp_c':current.get('temp_c'),
        'is_day':current.get('is_day'),
        'condition_text':condition.get('text'),
        'condition_icon':condition.get('icon'),
        'wind_kph':current.get('wind_kph'),
        'wind_degree':current.get('wind_degree'),
        'wind_dir':current.get('wind_dir'),
        'pressure_in':current.get('pressure_in'),
        'precip_in':current.get('precip_in'),
        'humidity':current.get('humidity'),
        'cloud':current.get('cloud'),
        'feelslike_c':current.get('feelslike_c'),
        'uv':current.get('uv'),
        'air-quality': {
            'co':airQuality.get('co'),
            'no2':airQuality.get('no2'),
            'o3':airQuality.get('o3'),
            'pm2_5':airQuality.get('pm2_5'),
            'pm10':airQuality.get('pm10'),
            'so2':airQuality.get('so2'),
            'us-epa-index':airQuality.get('us-epa-index'),
            'gb-defra-index':airQuality.get('gb-defra-index')
        },
        'alert':[
            {
                'headline':alert.get('headline'),
                'severity':alert.get('severity'),
                'desc':alert.get('desc'),
                'date':alert.get('date'),
                'category':alert.get('category'),
                'instructions':alert.get('instructions'),
            }
            for alert in alert
        ],
        'forecast':[
            {
                'date':forecast.get('date'),
                'maxtemp_c':forecast.get('day').get('maxtemp_c'),
                'mintemp_c':forecast.get('day'),
                'condition':forecast.get('day').get('condition').get('text'),
            }
            for forecast in forecast
        ]
    }
    return flatten_Data

def fetchData():

    # Getting secret value from key vault
    weatherApiKey = dbutils.secrets.get(scope = "key-vault-scope", key = "weatherapi")
    location = "Hyderabad"
    baseUrl = "http://api.weatherapi.com/v1/"

    currentWeather = getCurrentWeather(baseUrl, weatherApiKey, location)
    forecastWeather = getForecastWeather(baseUrl, weatherApiKey, location, 3)
    alertWeather = getAlertWeather(baseUrl, weatherApiKey, location)

    # print('Current Weather: \n', json.dumps(currentWeather, indent=3))
    # print('Forecast Weather: \n', json.dumps(forecastWeather, indent=3))
    # print('Alert Weather: \n', json.dumps(alertWeather, indent=3))
    mergedData = flattenData(currentWeather, forecastWeather, alertWeather)

    # print('Weather Data: \n', json.dumps(mergedData, indent=3))

    sendEvent(mergedData)

fetchData()

# COMMAND ----------

# MAGIC %md
# MAGIC # Sending Weather data in Streaming Fashion

# COMMAND ----------

# Top-level secret and constants (driver scope)
weatherApiKey = dbutils.secrets.get(scope="key-vault-scope", key="weatherapi")
even_hub_connection_string = dbutils.secrets.get(scope="key-vault-scope", key="eventHubConnectionString")
EVENT_HUB_NAME = "hub-weather-streams"
LOCATION = "Hyderabad"
BASE_URL = "http://api.weatherapi.com/v1"

# Setup Event Hub producer client at the driver level
from azure.eventhub import EventHubProducerClient, EventData
import json, requests



# API request handlers
def handle_response(response):
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error {response.status_code}: {response.text}")
        return {}

def getCurrentWeather(baseUrl, apiKey, location):
    url = f"{baseUrl}/current.json"
    params = {'key': apiKey, 'q': location, 'aqi': 'yes'}
    return handle_response(requests.get(url, params=params))

def getForecastWeather(baseUrl, apiKey, location, days):
    url = f"{baseUrl}/forecast.json"
    params = {'key': apiKey, 'q': location, 'days': days}
    return handle_response(requests.get(url, params=params))

def getAlertWeather(baseUrl, apiKey, location):
    url = f"{baseUrl}/alerts.json"
    params = {'key': apiKey, 'q': location}
    return handle_response(requests.get(url, params=params))

def flattenData(current, forecast, alerts):
    loc = current.get('location', {})
    curr = current.get('current', {})
    cond = curr.get('condition', {})
    air = curr.get('air_quality', {})
    fc = forecast.get('forecast', {}).get('forecastday', [])
    al = alerts.get('alerts', {}).get('alert', [])

    return {
        'name': loc.get('name'),
        'region': loc.get('region'),
        'country': loc.get('country'),
        'lat': loc.get('lat'),
        'lon': loc.get('lon'),
        'localtime': loc.get('localtime'),
        'temp_c': curr.get('temp_c'),
        'is_day': curr.get('is_day'),
        'condition_text': cond.get('text'),
        'condition_icon': cond.get('icon'),
        'wind_kph': curr.get('wind_kph'),
        'wind_degree': curr.get('wind_degree'),
        'wind_dir': curr.get('wind_dir'),
        'pressure_in': curr.get('pressure_in'),
        'precip_in': curr.get('precip_in'),
        'humidity': curr.get('humidity'),
        'cloud': curr.get('cloud'),
        'feelslike_c': curr.get('feelslike_c'),
        'uv': curr.get('uv'),
        'air_quality': {
            'co': air.get('co'), 'no2': air.get('no2'), 'o3': air.get('o3'),
            'pm2_5': air.get('pm2_5'), 'pm10': air.get('pm10'),
            'so2': air.get('so2'), 'us_epa_index': air.get('us-epa-index'),
            'gb_defra_index': air.get('gb-defra-index')
        },
        'alerts': [
            {
                'headline': a.get('headline'),
                'severity': a.get('severity'),
                'desc': a.get('desc'),
                'date': a.get('effective'),
                'category': a.get('category'),
                'instructions': a.get('instruction')
            } for a in al
        ],
        'forecast': [
            {
                'date': f.get('date'),
                'maxtemp_c': f.get('day', {}).get('maxtemp_c'),
                'mintemp_c': f.get('day', {}).get('mintemp_c'),
                'condition': f.get('day', {}).get('condition', {}).get('text')
            } for f in fc
        ]
    }

# Send data to Event Hub
def sendEvent(event):
    producer = EventHubProducerClient.from_connection_string(
    conn_str=even_hub_connection_string,
    eventhub_name=EVENT_HUB_NAME
)
    try:
        batch = producer.create_batch()
        batch.add(EventData(json.dumps(event)))
        producer.send_batch(batch)
    except Exception as e:
        print(f"Failed to send event: {e}")
    finally:
        producer.close()

# This runs in Spark job → pass in secrets/constants as arguments
def processBatch(batch_df, batch_id):
    try:
        current = getCurrentWeather(BASE_URL, weatherApiKey, LOCATION)
        forecast = getForecastWeather(BASE_URL, weatherApiKey, LOCATION, 3)
        alerts = getAlertWeather(BASE_URL, weatherApiKey, LOCATION)
        merged = flattenData(current, forecast, alerts)
        sendEvent(merged)
    except Exception as e:
        print(f"Error in batch {batch_id}: {e}")

# Streaming trigger
streaming_df = spark.readStream.format("rate").option("rowsPerSecond", 1).load()
query = streaming_df.writeStream.foreachBatch(processBatch).start()
query.awaitTermination()



# COMMAND ----------

# MAGIC %md
# MAGIC #Sending streams for every 30 seconds

# COMMAND ----------

# Setup Event Hub producer client at the driver level
from azure.eventhub import EventHubProducerClient, EventData
import json, requests
from datetime import datetime, timedelta

# Top-level secret and constants (driver scope)
weatherApiKey = dbutils.secrets.get(scope="key-vault-scope", key="weatherapi")
even_hub_connection_string = dbutils.secrets.get(scope="key-vault-scope", key="eventHubConnectionString")
EVENT_HUB_NAME = "hub-weather-streams"
LOCATION = "Hyderabad"
BASE_URL = "http://api.weatherapi.com/v1"


# API request handlers
def handle_response(response):
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error {response.status_code}: {response.text}")
        return {}

def getCurrentWeather(baseUrl, apiKey, location):
    url = f"{baseUrl}/current.json"
    params = {'key': apiKey, 'q': location, 'aqi': 'yes'}
    return handle_response(requests.get(url, params=params))

def getForecastWeather(baseUrl, apiKey, location, days):
    url = f"{baseUrl}/forecast.json"
    params = {'key': apiKey, 'q': location, 'days': days}
    return handle_response(requests.get(url, params=params))

def getAlertWeather(baseUrl, apiKey, location):
    url = f"{baseUrl}/alerts.json"
    params = {'key': apiKey, 'q': location}
    return handle_response(requests.get(url, params=params))

def flattenData(current, forecast, alerts):
    loc = current.get('location', {})
    curr = current.get('current', {})
    cond = curr.get('condition', {})
    air = curr.get('air_quality', {})
    fc = forecast.get('forecast', {}).get('forecastday', [])
    al = alerts.get('alerts', {}).get('alert', [])

    return {
        'name': loc.get('name'),
        'region': loc.get('region'),
        'country': loc.get('country'),
        'lat': loc.get('lat'),
        'lon': loc.get('lon'),
        'localtime': loc.get('localtime'),
        'temp_c': curr.get('temp_c'),
        'is_day': curr.get('is_day'),
        'condition_text': cond.get('text'),
        'condition_icon': cond.get('icon'),
        'wind_kph': curr.get('wind_kph'),
        'wind_degree': curr.get('wind_degree'),
        'wind_dir': curr.get('wind_dir'),
        'pressure_in': curr.get('pressure_in'),
        'precip_in': curr.get('precip_in'),
        'humidity': curr.get('humidity'),
        'cloud': curr.get('cloud'),
        'feelslike_c': curr.get('feelslike_c'),
        'uv': curr.get('uv'),
        'air_quality': {
            'co': air.get('co'), 'no2': air.get('no2'), 'o3': air.get('o3'),
            'pm2_5': air.get('pm2_5'), 'pm10': air.get('pm10'),
            'so2': air.get('so2'), 'us_epa_index': air.get('us-epa-index'),
            'gb_defra_index': air.get('gb-defra-index')
        },
        'alerts': [
            {
                'headline': a.get('headline'),
                'severity': a.get('severity'),
                'desc': a.get('desc'),
                'date': a.get('effective'),
                'category': a.get('category'),
                'instructions': a.get('instruction')
            } for a in al
        ],
        'forecast': [
            {
                'date': f.get('date'),
                'maxtemp_c': f.get('day', {}).get('maxtemp_c'),
                'mintemp_c': f.get('day', {}).get('mintemp_c'),
                'condition': f.get('day', {}).get('condition', {}).get('text')
            } for f in fc
        ]
    }

# Send data to Event Hub
def sendEvent(event):
    producer = EventHubProducerClient.from_connection_string(
    conn_str=even_hub_connection_string,
    eventhub_name=EVENT_HUB_NAME
)
    try:
        batch = producer.create_batch()
        batch.add(EventData(json.dumps(event)))
        producer.send_batch(batch)
    except Exception as e:
        print(f"Failed to send event: {e}")
    finally:
        producer.close()

# This runs in Spark job → pass in secrets/constants as arguments
lastSendTime = datetime.now() - timedelta(seconds=30)
def processBatch(batch_df, batch_id):
  global lastSendTime
  try:
      currentTime = datetime.now()
      if (currentTime - lastSendTime).total_seconds() >= 30:
        current = getCurrentWeather(BASE_URL, weatherApiKey, LOCATION)
        forecast = getForecastWeather(BASE_URL, weatherApiKey, LOCATION, 3)
        alerts = getAlertWeather(BASE_URL, weatherApiKey, LOCATION)
        merged = flattenData(current, forecast, alerts)
        sendEvent(merged)
        lastSendTime = currentTime
  except Exception as e:
      print(f"Error in batch {batch_id}: {e}")

# Streaming trigger
streaming_df = spark.readStream.format("rate").option("rowsPerSecond", 1).load()
query = streaming_df.writeStream.foreachBatch(processBatch).start()
query.awaitTermination()

