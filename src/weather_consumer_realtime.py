"""
weather_consumer_realtime.py

Kafka-konsument som läser väderdata i realtid från topic "weather-data"
och visualiserar temperatur och luftfuktighet per stad med en live
uppdaterad matplotlib-animation.

Använder ett eget group_id ("weather-consumer-realtime"), separat från
den befintliga batch-konsumenten ("weather-consumer-batch"), eftersom
varje consumer group tar emot en egen kopia av hela strömmen oberoende
av andra grupper. Detta gör att batch- och realtidskonsumenten kan köras
samtidigt utan att konkurrera om samma meddelanden.
"""

from datetime import datetime
import matplotlib.dates as mdates
import json
import matplotlib.pyplot as plt

from matplotlib.animation import FuncAnimation
from kafka import KafkaConsumer

from collections import defaultdict, deque

# Kafka-inställningar
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "weather-data"
GROUP_ID = "weather-consumer-realtime"

# Hur många senaste mätpunkter per stad som visas i grafen samtidigt.
# Äldre punkter faller ut automatiskt (deque med maxlen).
HISTORY_LENGTH = 20


def build_consumer():
    """
    Skapar och returnerar en KafkaConsumer konfigurerad för realtidsläsning
    av väderdata.

    - group_id är unikt för denna konsument, så att den får sin egen ström
      oberoende av weather-consumer-batch.
    - auto_offset_reset='latest' används medvetet (till skillnad från
      batch-konsumentens 'earliest'), eftersom vi vill visualisera NYA
      inkommande mätvärden live, inte rita upp all historik vid start.
    """
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id=GROUP_ID,
    )
    return consumer


def make_history_store():
    """
    Skapar och returnerar en datastruktur som håller reda på de senaste
    mätpunkterna per stad, för att kunna rita en live-graf.

    Strukturen är en defaultdict där varje stad automatiskt får tre
    tomma deques (temperatur, luftfuktighet, tidpunkter) första gången
    staden dyker upp i strömmen. deque med maxlen=HISTORY_LENGTH gör att
    endast de senaste mätpunkterna behålls, äldre trillar ut automatiskt.
    """
    return defaultdict(lambda: {
        "timestamps": deque(maxlen=HISTORY_LENGTH),
        "temperature": deque(maxlen=HISTORY_LENGTH),
        "humidity": deque(maxlen=HISTORY_LENGTH),
    })
    
    
def setup_figure(cities):
    """
    Skapar matplotlib-figuren med två subplots (temperatur och
    luftfuktighet), en linje per stad i varje subplot.

    Returnerar figur-objektet samt en dict med linjeobjekt per stad,
    så att update-funktionen kan uppdatera dem utan att rita om hela
    figuren från grunden varje gång (vilket vore långsamt).
    """
    fig, (ax_temp, ax_hum) = plt.subplots(2, 1, figsize=(9, 6), sharex=True)

    lines = {}
    for city in cities:
        (line_temp,) = ax_temp.plot([], [], label=city)
        (line_hum,) = ax_hum.plot([], [], label=city)
        lines[city] = {"temp": line_temp, "hum": line_hum}

    ax_temp.set_ylabel("Temperatur (°C)")
    ax_temp.legend(loc="upper left")
    ax_temp.set_title("Väderdata i realtid från Kafka")

    ax_hum.set_ylabel("Luftfuktighet (%)")
    ax_hum.set_xlabel("Tidpunkt")
    ax_hum.legend(loc="upper left")

    # Formaterar x-axeln som klockslag (HH:MM:SS) istället för
    # matplotlibs interna datumtalsrepresentation.
    time_formatter = mdates.DateFormatter("%H:%M:%S")
    ax_hum.xaxis.set_major_formatter(time_formatter)

    fig.autofmt_xdate()
    fig.tight_layout()

    return fig, (ax_temp, ax_hum), lines


def update_plot(frame, consumer, history, axes, lines):
    """
    Anropas av FuncAnimation för varje ny "frame". Läser tillgängliga
    nya meddelanden från Kafka utan att blockera (poll med kort
    timeout), uppdaterar historiken per stad och ritar om linjerna.

    Använder consumer.poll() istället för att iterera direkt över
    consumer, eftersom en blockerande for-loop skulle frysa
    matplotlib-fönstret i väntan på nästa Kafka-meddelande.
    """
    ax_temp, ax_hum = axes

    # Hämta alla väntande meddelanden just nu (max 1 sekunds väntan),
    # utan att blockera animationens uppdateringstakt i onödan.
    records = consumer.poll(timeout_ms=1000)

    for _topic_partition, messages in records.items():
        for message in messages:
            data = message.value
            city = data["city"]

            # Producern skickar timestamp som en ISO 8601-sträng
            # (t.ex. "2026-08-25T23:56:33.274549"). Vi parsar den till
            # ett riktigt datetime-objekt här, så matplotlib kan placera
            # punkterna korrekt i tid på x-axeln istället för att bara
            # räkna löpnummer 0, 1, 2...
            timestamp = datetime.fromisoformat(data["timestamp"])
            history[city]["timestamps"].append(timestamp)
            history[city]["temperature"].append(data["temperature"])
            history[city]["humidity"].append(data["humidity"])

    for city, series in history.items():
        if city not in lines:
            # Skydd ifall en stad dyker upp som inte fanns i CITIES
            # när grafen sattes upp (t.ex. om producern uppdateras
            # senare). Hoppar bara över ritning för den staden.
            continue

        x_values = list(series["timestamps"])
        lines[city]["temp"].set_data(x_values, series["temperature"])
        lines[city]["hum"].set_data(x_values, series["humidity"])

    ax_temp.relim()
    ax_temp.autoscale_view()
    ax_hum.relim()
    ax_hum.autoscale_view()

    return []


def main():
    """
    Startpunkt för realtidskonsumenten. Bygger Kafka-consumer, sätter
    upp matplotlib-figuren och startar en FuncAnimation som kontinuerligt
    pollar nya meddelanden och uppdaterar grafen.
    """
    cities = ["Stockholm", "Malmo", "Göteborg"]

    consumer = build_consumer()
    history = make_history_store()
    fig, axes, lines = setup_figure(cities)

    # interval=1000 (ms) styr hur ofta update_plot anropas, oavsett hur
    # ofta nya Kafka-meddelanden faktiskt kommer in. cache_frame_data=False
    # förhindrar en varning/minnesläckerisk eftersom vi inte har ett
    # fast antal frames (animationen körs "för evigt").
    _animation = FuncAnimation(
        fig,
        update_plot,
        fargs=(consumer, history, axes, lines),
        interval=1000,
        cache_frame_data=False,
    )

    plt.show()


if __name__ == "__main__":
    main()