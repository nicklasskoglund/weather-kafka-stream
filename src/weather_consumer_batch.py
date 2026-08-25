"""
weather_consumer_batch.py

Kafka-konsument (batch-läge) för väderdataprojektet.

Läser kontinuerligt väderdata från Kafka-topicet "weather-data" och
sparar varje mottaget meddelande som en rad i en CSV-fil under data/-mappen.
Syftet är att bygga upp ett historiskt dataset över tid, som senare kan
användas för analys, visualisering (feat/consumer-realtime) och
maskininlärning (feat/ml-prediction).

Del av kursmikroprojektet "Data streams" (Apache Kafka).
"""

import csv
import json
import os

from kafka import KafkaConsumer

KAFKA_TOPIC = "weather-data"
CSV_FILE_PATH = os.path.join("data", "weather_data.csv")
CSV_FIELDNAMES = ["city", "lat", "lon", "temperature", "humidity", "timestamp"]


def write_to_csv(weather_data, file_path=CSV_FILE_PATH):
    """
    Skriver en enskild väderdatapost som en rad till CSV-filen.

    Om filen inte redan finns skapas den och en rubrikrad läggs till
    (baserat på CSV_FIELDNAMES) innan datan skrivs.

    Args:
        weather_data (dict): En post med nycklarna definierade i
            CSV_FIELDNAMES (city, lat, lon, temperature, humidity, timestamp).
        file_path (str): Sökväg till CSV-filen. Standard: CSV_FILE_PATH.
    """
    file_exists = os.path.isfile(file_path)

    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, mode="a", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDNAMES)

        if not file_exists:
            writer.writeheader()

        writer.writerow(weather_data)


def build_consumer():
    """
    Skapar och returnerar en KafkaConsumer kopplad till localhost:9092,
    som lyssnar på KAFKA_TOPIC med JSON value_deserializer.

    auto_offset_reset="earliest" innebär att konsumenten, om den startas
    utan tidigare sparad offset, läser alla meddelanden från början av
    topicet (inte bara nya som kommer in efter start).
    """
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='weather-consumer-batch'
    )
    return consumer


def main():
    """
    Startar konsumenten och lyssnar kontinuerligt på KAFKA_TOPIC.
    Varje mottaget meddelande skrivs som en rad till CSV-filen via
    write_to_csv().
    """
    consumer = build_consumer()
    print(f"Lyssnar på topic '{KAFKA_TOPIC}'... (Ctrl+C för att avsluta)")

    for message in consumer:
        weather_data = message.value
        write_to_csv(weather_data)
        print(f"Sparade till CSV: {weather_data}")


if __name__ == "__main__":
    main()