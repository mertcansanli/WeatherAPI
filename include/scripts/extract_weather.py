import os
import requests
from datetime import datetime, timezone


def extract_weather(city: str) -> dict:
    api_key = os.getenv("OPENWEATHER_API_KEY")

    if not api_key:
        raise ValueError("OPENWEATHER_API_KEY is missing.")

    url = "https://api.openweathermap.org/data/2.5/weather"

    params = {
        "q": city,
        "appid": api_key,
        "units": "metric"
    }

    response = requests.get(url, params=params, timeout=20)

    if response.status_code != 200:
        raise Exception(
            f"OpenWeather API request failed. "
            f"Status code: {response.status_code}, Response: {response.text}"
        )

    data = response.json()

    return {
        "city_requested": city,
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "source": "openweather",
        "raw": data
    }