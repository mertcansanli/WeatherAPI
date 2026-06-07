def transform_weather(payload:dict) -> dict:
    raw = payload["raw"]

    main = raw.get("main", {})
    wind = raw.get("wind", {})
    weather_list = raw.get("weather", [])
    weather = weather_list[0] if weather_list else {}
    collected_at = payload.get("collected_at")
    collected_hour = collected_at[:13] + ":00:00" if collected_at else None

    transformed = {
        "city": raw.get("name"),
        "country": raw.get("sys", {}).get("country"),
        "temperature_c": main.get("temp"),
        "feels_like_c": main.get("feels_like"),
        "humidity": main.get("humidity"),
        "pressure": main.get("pressure"),
        "wind_speed": wind.get("speed"),
        "weather_main": weather.get("main"),
        "weather_description": weather.get("description"),
        "collected_at": collected_at,
        "collected_hour": collected_hour,
    }

    return transformed