import logging
import azure.functions as func
import json
import requests
from azure.eventhub import EventHubProducerClient, EventData
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


app = func.FunctionApp()


@app.timer_trigger(schedule="*/30 * * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def WeatherAPIFunction(myTimer: func.TimerRequest) -> None:
    
    if myTimer.past_due:
        logging.info('The timer is past due!')
    
    if myTimer.past_due:
        logging.info('The timer is past due!')

    # Event Hub Config
    EVENT_HUB_NAME = "hub-weather-streams"
    EVENT_HUB_NAMESPACE = "eh-weather-streamer.servicebus.windows.net"#HOST NAME 
    
    # USE MANAGED IDENTITY OF FUNCTION APP FOR AUTHENTICATION
    credentials = DefaultAzureCredential()

    # Create producer client
    producer = EventHubProducerClient(
        fully_qualified_namespace=EVENT_HUB_NAMESPACE,
        eventhub_name=EVENT_HUB_NAME,
        credential=credentials
    )

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
    def getCurrentWeather(baseURL, weatherApiKey, location):
        currentWeather = f"{baseURL}/current.json"
        params ={
        'key': weatherApiKey,
        'q': location,
        'aqi': 'yes'
        }
        response = requests.get(currentWeather, params=params)
        return handle_response(response)

    # Get Forecast Weather
    def getForecastWeather(baseURL, weatherApiKey, location,days):
        forecastWeather = f"{baseURL}/forecast.json"
        params ={
        'key': weatherApiKey,
        'q': location,
        'days': days
        }
        response = requests.get(forecastWeather, params=params)
        return handle_response(response)

    # Get Alerts
    def getAlertWeather(baseURL, weatherApiKey, location):
        alertWeather = f"{baseURL}/alerts.json"
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

    # GET SECRET FROM KEY VAULT
    def getSecretApiKey(keyVaultUrl, secretName):
        credential = DefaultAzureCredential()
        secret_Client = SecretClient(vault_url=keyVaultUrl, credential=credential)
        retrivedSecret = secret_Client.get_secret(secretName)
        return retrivedSecret.value

    def fetchData():
        #Key Vault Details
        VAULT_URL = "https://kv-weather-streamers.vault.azure.net/"
        API_KEY_SECRET_NAME = "weatherapi" 
        weatherApiKey = getSecretApiKey(VAULT_URL, API_KEY_SECRET_NAME)

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

        sendEvent(mergedData)

    fetchData()
    producer.close()

    logging.info('Python timer trigger function executed.')