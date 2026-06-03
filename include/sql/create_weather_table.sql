CREATE TABLE IF NOT EXISTS weather_observations (
    id SERIAL PRIMARY KEY,
    city TEXT NOT NULL,
    country TEXT,
    temperature_c NUMERIC,
    feels_like_c NUMERIC,
    humidity INTEGER,
    pressure INTEGER,
    wind_speed NUMERIC,
    weather_main TEXT,
    weather_description TEXT,
    collected_at TIMESTAMP,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);