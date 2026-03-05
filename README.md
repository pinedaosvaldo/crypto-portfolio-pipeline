# Crypto Portfolio Data Pipeline

Data pipeline that collects Bitso portfolio snapshots, stores them in BigQuery and visualizes them in Looker Studio with ARIMA forecasting.

## Architecture

Bitso API → Python (Colab) → BigQuery → BigQuery ML (ARIMA) → Looker Studio

## Tech Stack
- Python
- Bitso API
- BigQuery
- BigQuery ML
- Looker Studio

## Features
- Daily portfolio snapshots
- ROI and PNL calculations
- Token growth from staking
- Forecasts for 30, 60, and 90 days

## Dashboard
Looker Studio dashboard:
https://lookerstudio.google.com/reporting/f6fa9420-4b3d-40fc-9f59-a601405875bf

## Project Structure
crypto-portfolio-pipeline
│
├── notebooks
│   ├── bitso_snapshot.ipynb
│   ├── bigquery_insert.ipynb
│   └── arima_forecast.ipynb
│
├── sql
│   └── forecasting_queries.sql
│
├── images
│   └── architecture.png
│
└── README.md
