"""
weather_producer.py

Kafka-producer för väderdataprojektet.

Hämtar aktuell väderdata (temperatur, luftfuktighet) för ett antal svenska
städer via OpenWeatherMap API, och publicerar datan kontinuerligt till
Kafka-topicet "weather-data" för vidare konsumtion (batch-lagring och/eller
realtidsvisualisering).

Del av kursmikroprojektet "Data streams" (Apache Kafka).
"""

import json
import os
import time
from datetime import datetime

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

CITIES = ["Stockholm", "Malmo", "Göteborg"]
POLL_INTERVAL_SECONDS = 60
KAFKA_TOPIC = "weather-data"


def fetch_weather(city):
    """
    Hämtar aktuellt väder för en given stad via OpenWeatherMap API.

    Returnerar en dict med minst:
        - city
        - lat
        - lon
        - temperature
        - humidity
        - timestamp
    Returnerar None om anropet misslyckas.
    """

    api_key = os.getenv("OPENWEATHER_API_KEY")
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": city,
        "appid": api_key,
        "units": "metric"
    }

    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.json()
        weather_data = {
            "city": city,
            "lat": data["coord"]["lat"],
            "lon": data["coord"]["lon"],
            "temperature": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "timestamp": datetime.now().isoformat()
        }
        return weather_data
    else:
        print(f"Fel vid hämtning av väder för {city}: statuskod {response.status_code}")
        return None


def build_producer():
    """
    Skapar och returnerar en KafkaProducer kopplad till localhost:9092,
    med JSON value_serializer.
    """
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    return producer


def main():
    """
    Loopar över CITIES var POLL_INTERVAL_SECONDS:e sekund,
    hämtar väderdata och skickar till Kafka-topic KAFKA_TOPIC.
    """
    producer = build_producer()

    while True:
        for city in CITIES:
            weather_data = fetch_weather(city)
            if weather_data is not None:
                producer.send(KAFKA_TOPIC, weather_data)
                print(f"Skickade data för {city}: {weather_data}")
            else:
                print(f"Hoppar över {city} p.g.a. fel vid hämtning.")
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()