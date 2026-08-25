# weather-kafka-stream

Real-time weather data streaming using the OpenWeatherMap API and Apache Kafka.

## Setup

1. Create and activate a virtual environment:

       python -m venv .venv
       source .venv/Scripts/activate   # Git Bash

2. Install dependencies:

       pip install -r requirements.txt

3. Get a free API key from https://openweathermap.org/api
4. Start Zookeeper and Kafka locally
5. Run the producer and consumer scripts (see src/)

## Project structure

    weather-kafka-stream/
    ├── src/
    │   ├── weather_producer.py
    │   ├── weather_consumer_batch.py
    │   └── weather_consumer_realtime.py
    ├── data/                  (CSV output, gitignored)
    ├── requirements.txt
    ├── .gitignore
    ├── LICENSE
    └── README.md

## License

MIT — see LICENSE
EOF