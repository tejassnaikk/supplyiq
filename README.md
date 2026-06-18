# SupplyIQ — End-to-End Supply Chain Intelligence Platform

A production-grade supply chain analytics and demand forecasting platform built using **Databricks**, **Snowflake**, and **Power BI**.

## Architecture
[Kaggle Supply Chain Dataset]

↓

[Databricks — Bronze Layer]     Raw ingestion into Delta Lake

↓

[Databricks — Silver Layer]     Cleaning, typing, derived features

↓

[Databricks — Gold Layer]       SKU-level weekly demand aggregations

↓

[Snowflake — Data Warehouse]    GOLD + ML_OUTPUTS schemas

↓

[Databricks — ML Layer]         Prophet demand forecasting (118 products)

↓

[Power BI Service — Dashboards] 3 interactive business dashboards

## Tech Stack

| Tool | Role |
|---|---|
| Databricks (Free Edition) | Data processing, Delta Lake, ML modeling |
| Snowflake (Standard Trial) | Cloud data warehouse |
| Power BI Service | Business intelligence dashboards |
| Prophet | Time series demand forecasting |
| Python / PySpark | Data transformation and ML |

## Project Structure
supplyiq/

├── notebooks/

│   ├── 01_bronze_ingestion.py

│   ├── 02_silver_transform.py

│   ├── 03_gold_demand_aggregation.py

│   ├── 04_push_to_snowflake.py

│   ├── 05_demand_forecasting.py

│   └── 06_unified_table.py

├── docs/

└── README.md

## Dashboards

- **Demand Forecast Hub** — Actual vs forecasted demand per product
- **Market & Category Performance** — Revenue and profit by market and category
- **Demand Forecast** — Weekly demand trends with Prophet 4-week forward forecasts

## Data

- **Source:** DataCo Smart Supply Chain Dataset (Kaggle)
- **Size:** 180,519 order records
- **Date Range:** 2014-2018
- **Products:** 118 unique SKUs across multiple markets

## Key Results

- Built full medallion architecture (Bronze to Silver to Gold) on Databricks
- Aggregated 180K order rows into 8,440 SKU-week demand records
- Trained Prophet forecasting models on 118 products
- Generated 348 forward forecast rows across all products
- Delivered 3 interactive Power BI dashboards connected to Snowflake

## Setup

1. Upload DataCoSupplyChainDataset.csv to Databricks Volume
2. Run notebooks in order: 01 to 06
3. Connect Power BI Service to Snowflake SUPPLYIQ.ML_OUTPUTS.UNIFIED_DEMAND
4. Open SupplyIQ_v2 report in Power BI Service

## Author

Tejas Snaikk — MS Data Science, CU Boulder
