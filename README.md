# Weather Pipeline

A small Python project that fetches weather data from Open-Meteo, provides a lightweight FastAPI surface, and includes utilities to transform and insert data into a Postgres database.

## Features

- Geocoding using Open-Meteo geocoding API
- Fetch hourly and daily weather (archive API)
- FastAPI endpoints for quick `current` and `forecast` lookups
- Helpers to transform data for Postgres insertion

## Quickstart

1. Create a virtual environment and install dependencies:

```bash
python -m venv .venv
.
# Windows
.\.venv\Scripts\activate
pip install -r requirements.txt
```

2. Run the FastAPI server (development):

```bash
pip install "uvicorn[standard]"
uvicorn app.api:app --reload --host 127.0.0.1 --port 8000
```

3. Example requests:

```bash
# Current weather
curl "http://127.0.0.1:8000/current?city=Beckley"

# 5-day forecast
curl "http://127.0.0.1:8000/forecast?city=Beckley"
```

## Project layout

- `app/` — FastAPI app and CLI helpers (`api.py`, `main.py`)
- `ingest/` — geocoding and data fetching (`fetch.py`, `geocode.py`, `save_to_json.py`)
- `database/` — transform and insert helpers (`transform_db.py`, `insert.py`)
- `data/`, `ingest/data/` — sample and cached JSON/CSV outputs

## Environment

- To run DB insertion features, set the following environment variables:

```bash
export PG_HOST=your_host
export PG_USER=your_user
export PG_PASSWORD=your_password
```

On Windows (PowerShell):

```powershell
$env:PG_HOST = "your_host"
$env:PG_USER = "your_user"
$env:PG_PASSWORD = "your_password"
```

## Notes & Recommendations

- The FastAPI endpoints live in `app/api.py`. They accept a `city` query parameter and return JSON for `current` and `forecast`.
- The ingest client uses cached requests and retry logic for robustness (`ingest/fetch.py`).
- Database insertion expects a Postgres `weather` database and the schema matching the inserter in `database/insert.py`.
- If you plan to expose this service publicly, add proper rate-limiting, caching, and API key protection.

## Contributing

Feel free to open issues or PRs. For changes to the data model, update `database/transform_db.py` and the `INSERT` ordering in `database/insert.py` together.

## License

MIT-style (no license file included by default).
