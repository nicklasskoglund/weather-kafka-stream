# weather-kafka-stream

Real-time weather data streaming using the OpenWeatherMap API and Apache Kafka.

The project includes a producer, two consumers (batch and real-time), and a
temperature prediction module built on top of the collected data.

## Features

- **Producer** — fetches temperature, humidity, and coordinates for a set of
  cities from OpenWeatherMap and publishes them to a Kafka topic every 60
  seconds.
- **Batch consumer** — reads the full topic history and appends every message
  to a CSV file for long-term storage.
- **Real-time consumer** — reads only newly arriving messages and renders a
  live-updating matplotlib chart (temperature and humidity per city).
- **Temperature predictor** — trains a simple linear regression model per city
  on the collected CSV history and predicts the next temperature value. Runs
  once at the start of the real-time consumer, prints the prediction to the
  terminal, and logs it to a separate CSV for later evaluation.

## Setup

1. Create and activate a virtual environment:

       python -m venv .venv
       source .venv/Scripts/activate   # Git Bash

2. Install dependencies:

       pip install -r requirements.txt

3. Get a free API key from https://openweathermap.org/api and add it to a
   `.env` file in the project root:

       OPENWEATHER_API_KEY=your_key_here

   See `.env.example` for the expected format.

4. Start a local Kafka broker (KRaft mode, no Zookeeper required — see
   *Kafka setup* below) and make sure the `weather-data` topic exists.

5. Run the scripts as modules from the project root (not from inside `src/`),
   since `weather_consumer_realtime.py` imports `temperature_predictor` as a
   package:

       python -m src.weather_producer
       python -m src.weather_consumer_batch
       python -m src.weather_consumer_realtime
       python -m src.temperature_predictor   # optional, standalone test

## Kafka setup

This project runs Kafka in **KRaft mode** rather than with Zookeeper, since
Zookeeper support has been removed from recent Kafka releases. Producer and
consumer code and behavior are unaffected — only the broker startup procedure
differs from the classic Zookeeper-based setup.

## Project structure

    weather-kafka-stream/
    ├── src/
    │   ├── weather_producer.py
    │   ├── weather_consumer_batch.py
    │   ├── weather_consumer_realtime.py
    │   └── temperature_predictor.py
    ├── data/                       (gitignored)
    │   ├── weather_data.csv        (written by the batch consumer)
    │   └── predictions_log.csv     (written by the temperature predictor)
    ├── requirements.txt
    ├── .env.example
    ├── .gitignore
    ├── LICENSE
    └── README.md

## License

MIT — see LICENSE