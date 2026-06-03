def validate_weather(record:dict) -> None:
    required_field = [
        "city", "temperature_c","humidity","pressure","collected_at"
    ]

    missin_fields= [
        field for field in required_field
        if record.get(field) is None
    ]
    if missin_fields == True:
        raise ValueError (f" These Fields are missing!  : {missin_fields}")

    temperature = record["temperature_c"]
    humidity = record["humidity"]

    if temperature < -90 or temperature > 60:
        raise ValueError(f"Suspicious temperature value: {temperature}")

    if humidity < 0 or humidity > 100:
        raise ValueError(f"Invalid humidity value: {humidity}")