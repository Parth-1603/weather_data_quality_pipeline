-- Run these queries in DuckDB against data/weather.duckdb

-- 1. City-level conditions across all collected snapshots.
SELECT city,
       ROUND(AVG(temperature_c), 2) AS avg_temperature_c,
       ROUND(AVG(relative_humidity_pct), 2) AS avg_humidity_pct,
       ROUND(AVG(precipitation_mm), 2) AS avg_precipitation_mm,
       ROUND(AVG(wind_speed_kmh), 2) AS avg_wind_speed_kmh,
       ROUND(MAX(temperature_c), 2) AS max_temperature_c,
       ROUND(MIN(temperature_c), 2) AS min_temperature_c,
       COUNT(*) AS observations
FROM clean_weather
GROUP BY city
ORDER BY avg_temperature_c DESC;

-- 2. Daily temperature and wind trend.
SELECT CAST(observed_at AS DATE) AS observation_date,
       COUNT(*) AS observations,
       ROUND(AVG(temperature_c), 2) AS avg_temperature_c,
       ROUND(MAX(temperature_c), 2) AS max_temperature_c,
       ROUND(MIN(temperature_c), 2) AS min_temperature_c,
       ROUND(AVG(wind_speed_kmh), 2) AS avg_wind_speed_kmh
FROM clean_weather
GROUP BY CAST(observed_at AS DATE)
ORDER BY observation_date;

-- 3. Quality & Rejection monitoring (quarantine breakdown).
SELECT quality_issues,
       COUNT(*) AS count
FROM rejected_weather
GROUP BY quality_issues
ORDER BY count DESC;

-- 4. Diurnal Temperature Variation (Max - Min temp range per city).
SELECT city,
       ROUND(MAX(temperature_c) - MIN(temperature_c), 2) AS diurnal_temp_range_c,
       ROUND(AVG(relative_humidity_pct), 2) AS avg_humidity_pct,
       COUNT(*) AS observations
FROM clean_weather
GROUP BY city
ORDER BY diurnal_temp_range_c DESC;

-- 5. Operational threshold monitoring.
SELECT
    COUNT(*) FILTER (WHERE precipitation_mm > 0) AS observations_with_precipitation,
    COUNT(*) FILTER (WHERE wind_speed_kmh >= 25) AS high_wind_observations,
    COUNT(*) FILTER (WHERE temperature_c >= 35) AS hot_observations,
    COUNT(*) AS total_observations
FROM clean_weather;

-- 6. Snapshot freshness.
SELECT city, MAX(observed_at) AS latest_observation
FROM clean_weather
GROUP BY city
ORDER BY city;
