from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import os

# Coordonnées des villes ciblées
CITY_COORDS = {
    "Paris": {"lat": 48.85, "lon": 2.35},
    "Londres": {"lat": 51.51, "lon": -0.13},
    "Berlin": {"lat": 52.52, "lon": 13.40}
}

# Définition du chemin du fichier CSV de destination
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "weather_data.csv")
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

def fetch_weather_data():
    weather_entries = []
    for city, coord in CITY_COORDS.items():
        api_url = (
            f"https://api.open-meteo.com/v1/forecast?"
            f"latitude={coord['lat']}&longitude={coord['lon']}&current_weather=true"
        )
        response = requests.get(api_url)
        current_weather = response.json().get("current_weather", {})
        weather_entries.append({
            "city": city,
            "timestamp": datetime.utcnow().isoformat(),
            "temperature": current_weather.get("temperature"),
            "windspeed": current_weather.get("windspeed"),
            "weathercode": current_weather.get("weathercode")
        })
    
    df = pd.DataFrame(weather_entries)
    df.to_csv("/tmp/temp_weather.csv", index=False)

def save_weather_data():
    temp_path = "/tmp/temp_weather.csv"
    if not os.path.exists(temp_path):
        return

    new_data = pd.read_csv(temp_path)

    if os.path.exists(OUTPUT_FILE):
        existing_data = pd.read_csv(OUTPUT_FILE)
        full_data = pd.concat([existing_data, new_data], ignore_index=True)
        full_data = full_data.drop_duplicates(
            subset=["city", "temperature", "windspeed", "weathercode"],
            keep="last"
        )
    else:
        full_data = new_data

    full_data.to_csv(OUTPUT_FILE, index=False)

# Définition du DAG Airflow
with DAG(
    dag_id="daily_weather_pipeline",
    schedule_interval="0 8 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["weather", "ETL"]
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_weather",
        python_callable=fetch_weather_data
    )

    save_task = PythonOperator(
        task_id="save_weather",
        python_callable=save_weather_data
    )

    fetch_task >> save_task
