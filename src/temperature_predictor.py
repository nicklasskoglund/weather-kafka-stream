"""
temperature_predictor.py

Fristående ML-modul för temperaturprediktion i väderdataströms-projektet.

Tränar en enkel linjär regressionsmodell per stad utifrån historisk data i
data/weather_data.csv (skriven av weather_consumer_batch.py), och kan sedan
prediktera nästa temperaturvärde för en given stad.

Modulen är medvetet oberoende av hur resultatet visas (terminal, graf, framtida
UI etc.) — den enda uppgiften här är att träna modeller och returnera siffror.
Detta gör det enkelt att återanvända logiken i t.ex. ett framtida API-lager
utan att skriva om ML-koden.

Modellen tränas engångsvis (vid anrop av train_models), inte successivt under
körning — se projektets README/PR-beskrivning för resonemang kring detta val.
"""

import csv
import os
from datetime import datetime, timezone

import pandas as pd
from sklearn.linear_model import LinearRegression

# Samma sökväg som batch-konsumenten skriver till.
CSV_FILE_PATH = "data/weather_data.csv"
PREDICTIONS_LOG_PATH = "data/predictions_log.csv"
PREDICTIONS_LOG_FIELDNAMES = ["city", "lat", "lon", "predicted_temperature", "timestamp"]


def train_models(csv_path=CSV_FILE_PATH):
    """
    Tränar en LinearRegression-modell per stad utifrån historisk data i CSV-filen.

    Använder radens löpnummer inom respektive stad (0, 1, 2, ...) som enda
    prediktor (X), och temperature som målvariabel (y). Detta fungerar bra här
    eftersom producern skickar data med jämn tidsintervall (POLL_INTERVAL_SECONDS),
    så löpnumret är en rimlig proxy för tidsförlopp utan att vi behöver parsa
    och normalisera timestamp-strängar.

    Args:
        csv_path (str): Sökväg till CSV-filen med väderdata.

    Returns:
        dict: {city: {"model": LinearRegression, "next_index": int}}
              next_index är det index modellen ska prediktera näst (dvs.
              antalet observationer staden redan har).
              Returnerar en tom dict om filen saknas eller är tom.
    """
    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        print(f"[temperature_predictor] Hittade ingen CSV-fil på {csv_path}.")
        return {}

    if df.empty:
        print("[temperature_predictor] CSV-filen är tom, inget att träna på.")
        return {}

    models = {}

    for city, group in df.groupby("city"):
        # Behöver minst två punkter för att en linjär regression ska vara meningsfull.
        if len(group) < 2:
            print(f"[temperature_predictor] Hoppar över {city}: för få datapunkter.")
            continue

        # X = löpnummer 0..n-1, formaterat som kolumnvektor (sklearn kräver 2D-array).
        x_values = [[i] for i in range(len(group))]
        y_values = group["temperature"].tolist()

        model = LinearRegression()
        model.fit(x_values, y_values)

        models[city] = {
            "model": model,
            "next_index": len(group),
            "lat": group["lat"].iloc[-1],
            "lon": group["lon"].iloc[-1],
        }

    return models


def predict_next_temperature(models, city):
    """
    Prediktera nästa temperaturvärde för en given stad.

    Args:
        models (dict): Resultatet från train_models().
        city (str): Stadens namn, t.ex. "Stockholm".

    Returns:
        float | None: Predikterat temperaturvärde, eller None om staden
                       saknar en tränad modell (t.ex. för få datapunkter
                       eller staden dyker inte upp i CSV:n).
    """
    if city not in models:
        return None

    model = models[city]["model"]
    next_index = models[city]["next_index"]

    prediction = model.predict([[next_index]])
    return prediction[0]


def log_prediction(city, lat, lon, predicted_temperature, log_path=PREDICTIONS_LOG_PATH):
    """
    Appendar en rad med en gjord prediktion till predictions_log.csv.

    Loggar prediktionen tillsammans med koordinater och en tidsstämpel (UTC)
    för att i efterhand kunna jämföra predikterat värde mot det faktiska värde
    som senare kommer in via weather_data.csv (utvärdering ej implementerad
    ännu, men datan finns tillgänglig för det). Lat/lon inkluderas för
    konsekvens med weather_data.csv och i förberedelse för samma framtida
    skalning till godtyckliga koordinater som resten av projektet redan
    förbereder för.

    Skapar filen (med rubrikrad) och data/-mappen vid första anropet, precis
    som write_to_csv() i weather_consumer_batch.py.

    Args:
        city (str): Staden prediktionen gäller.
        lat (float): Latitud för staden.
        lon (float): Longitud för staden.
        predicted_temperature (float): Det predikterade temperaturvärdet.
        log_path (str): Sökväg till loggfilen.
    """
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    file_exists = os.path.isfile(log_path)

    with open(log_path, mode="a", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=PREDICTIONS_LOG_FIELDNAMES)

        if not file_exists:
            writer.writeheader()

        writer.writerow({
            "city": city,
            "lat": lat,
            "lon": lon,
            "predicted_temperature": round(predicted_temperature, 2),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })


if __name__ == "__main__":
    trained_models = train_models()
    print("Tränade modeller för:", list(trained_models.keys()))
    for city in trained_models:
        prediction = predict_next_temperature(trained_models, city)
        lat = trained_models[city]["lat"]
        lon = trained_models[city]["lon"]
        print(f"{city}: predikterad nästa temperatur = {prediction:.2f}°C")
        log_prediction(city, lat, lon, prediction)