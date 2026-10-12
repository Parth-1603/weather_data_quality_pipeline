# Live Weather Data Quality and OLAP Analytics Pipeline
**Semester 5 — Data Engineering Mini Project (DE MP)**

A high-performance Python + DuckDB data engineering pipeline that fetches real-time and multi-day hourly weather data from the public Open-Meteo API, applies rigorous data quality checks, quarantines defective observations, stores records in an embedded DuckDB columnar store, executes analytical OLAP queries, and generates reports and visualizations.

---

## Key Features

- **Free Public API**: Uses Open-Meteo (no API key or credentials required).
- **Multi-City Telemetry**: Ingests weather metrics for Mumbai, Delhi, Bengaluru, Chennai, Pune, and Kolkata.
- **Data Quality & Quarantine Pattern**: Validates completeness, numeric boundaries, non-negativity, and uniqueness; quarantines anomalies with failure annotations.
- **Embedded Columnar OLAP**: Uses DuckDB for sub-millisecond analytical aggregations on local hardware.
- **Automated Unit Testing**: Includes `unittest` test suite covering all data quality dimensions.
- **Rich Visualizations**: Generates multi-panel comparative charts, time-series curves, and quality breakdown donuts.
- **Submission-Ready Report**: Includes an academic project report with viva preparation questions.

---

## Quick Start (Windows PowerShell)

```powershell
# 1. Activate the Python virtual environment
.\.venv\Scripts\Activate.ps1

# 2. Run the full pipeline (fetches live & multi-day data, runs quality checks, stores in DuckDB, generates reports)
python src/pipeline.py

# 3. (Optional) Run the unit tests
python -m unittest tests/test_quality.py

# 4. (Optional) Run an interactive DuckDB OLAP query
python -c "import duckdb; c=duckdb.connect('data/weather.duckdb', read_only=True); print(c.sql('SELECT city, ROUND(AVG(temperature_c), 2) AS avg_temp_c, COUNT(*) AS observations FROM clean_weather GROUP BY city ORDER BY avg_temp_c DESC').df()); c.close()"
```

---

## Command Options

| Command | Description |
| :--- | :--- |
| `python src/pipeline.py` | Full execution (live + 8-day hourly data + dirty row test injection) |
| `python src/pipeline.py --reset-db` | Drops existing DuckDB tables and recreates them from a fresh ingestion |
| `python src/pipeline.py --mode snapshot --no-dirty` | Runs a lightweight 6-observation live snapshot without test anomalies |
| `python -m unittest tests/test_quality.py` | Runs the automated data quality test suite |

---

## Project Structure

```text
weather_data_quality_pipeline/
│
├── src/
│   └── pipeline.py                 # Core ETL & Data Quality Pipeline
│
├── tests/
│   └── test_quality.py             # Unit tests for data quality rules
│
├── sql/
│   └── olap_queries.sql            # Analytical OLAP queries for DuckDB
│
├── data/
│   ├── raw/                        # Immutable raw JSON payloads & normalized CSV
│   ├── clean/clean_weather.csv     # Accepted and validated records
│   ├── quarantine/rejected_weather.csv  # Quarantined anomalous records
│   └── weather.duckdb              # Embedded columnar analytical database
│
├── reports/
│   ├── PROJECT_REPORT.md           # Comprehensive academic submission report
│   ├── final_report.md             # Markdown summary report
│   ├── quality_report.csv          # Data quality pass rate & metrics
│   ├── quarantine_summary.csv      # Quarantine failure root-cause counts
│   ├── city_weather_summary.csv    # City-level aggregation metrics
│   ├── daily_weather_summary.csv   # Daily temperature and wind trends
│   ├── condition_summary.csv       # Operational threshold indicators
│   ├── performance_report.csv      # Actual execution timings on laptop
│   ├── temperature_by_city.png     # Bar chart of average temperatures
│   ├── weather_metrics_comparison.png # 3-panel Temp/Humidity/Wind comparison
│   ├── daily_temperature_trend.png # Time-series min/avg/max temperature plot
│   └── data_quality_breakdown.png  # Donut chart of pass vs quarantine rate
│
├── k8s/
│   ├── README.md                   # Kubernetes deployment instructions
│   └── weather-pipeline.yaml       # Kubernetes CronJob manifest
│
├── Dockerfile                      # Container definition for containerized deployment
├── requirements.txt                # Python dependencies
└── README.md                       # Project overview and usage guide
```

---

## Data Quality Rules Enforced

1. **Mandatory Presence**: Required fields must not be empty or null.
2. **Numeric Validity**: Numeric fields must parse cleanly to floating-point numbers.
3. **Temperature Bounds**: Plausible physical temperature ($-90.0^\circ\text{C}$ to $65.0^\circ\text{C}$).
4. **Humidity Bounds**: Relative humidity ($0\%$ to $100\%$).
5. **Precipitation Non-negativity**: Precipitation cannot be negative ($\ge 0\text{ mm}$).
6. **Wind Speed Non-negativity**: Wind speed cannot be negative ($\ge 0\text{ km/h}$).
7. **Deduplication**: Composite key on `(city, observed_at)` deduplicated by latest ingestion timestamp.
