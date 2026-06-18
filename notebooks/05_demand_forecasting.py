# SupplyIQ - Notebook 5: Demand Forecasting
# Purpose: Train Prophet models for all 118 products and push forecasts to Snowflake

import snowflake.connector
import pandas as pd
import numpy as np
from prophet import Prophet
import warnings
warnings.filterwarnings('ignore')

SNOWFLAKE_ACCOUNT = "rlbhiky-di18278"
SNOWFLAKE_DATABASE = "SUPPLYIQ"
SNOWFLAKE_WAREHOUSE = "SUPPLYIQ_WH"

# Load demand data from Snowflake
conn = snowflake.connector.connect(
    account=SNOWFLAKE_ACCOUNT,
    user="YOUR_USERNAME",
    password="YOUR_PASSWORD",
    database=SNOWFLAKE_DATABASE,
    schema="GOLD",
    warehouse=SNOWFLAKE_WAREHOUSE
)

cursor = conn.cursor()
cursor.execute("SELECT * FROM DEMAND_WEEKLY ORDER BY PRODUCT_NAME, WEEK_START")
df = pd.DataFrame(cursor.fetchall(), columns=[d[0] for d in cursor.description])
cursor.close()
conn.close()

df.columns = df.columns.str.lower()
df['week_start'] = pd.to_datetime(df['week_start'])
print(f"Loaded {df.shape[0]} rows, {df['product_name'].nunique()} products")

# Train Prophet for all products
results = []
for i, product in enumerate(df['product_name'].unique()):
    try:
        df_product = df[df['product_name'] == product][['week_start', 'total_units_sold']].copy()
        df_product.columns = ['ds', 'y']
        df_product = df_product.sort_values('ds').dropna()

        if len(df_product) < 10:
            continue

        model = Prophet(yearly_seasonality=True, weekly_seasonality=False, daily_seasonality=False, changepoint_prior_scale=0.05)
        model.fit(df_product)

        future = model.make_future_dataframe(periods=4, freq='W')
        forecast = model.predict(future)

        last_date = df_product['ds'].max()
        future_fc = forecast[forecast['ds'] > last_date][['ds', 'yhat', 'yhat_lower', 'yhat_upper']].copy()
        future_fc['product_name'] = product
        future_fc['forecast_generated_at'] = pd.Timestamp.now()
        future_fc['model'] = 'prophet'
        results.append(future_fc)

        if (i + 1) % 20 == 0:
            print(f"Trained {i+1} products...")

    except Exception as e:
        print(f"Skipped {product}: {e}")

df_forecasts = pd.concat(results, ignore_index=True)
df_forecasts.columns = ['forecast_date', 'yhat', 'yhat_lower', 'yhat_upper', 'product_name', 'forecast_generated_at', 'model']
df_forecasts['yhat'] = df_forecasts['yhat'].clip(lower=0).round(2)
df_forecasts['yhat_lower'] = df_forecasts['yhat_lower'].clip(lower=0).round(2)
df_forecasts['yhat_upper'] = df_forecasts['yhat_upper'].round(2)

print(f"Total forecasts: {len(df_forecasts)}")

# Push to Snowflake
conn = snowflake.connector.connect(
    account=SNOWFLAKE_ACCOUNT,
    user="YOUR_USERNAME",
    password="YOUR_PASSWORD",
    database=SNOWFLAKE_DATABASE,
    schema="ML_OUTPUTS",
    warehouse=SNOWFLAKE_WAREHOUSE
)

cursor = conn.cursor()
cursor.execute("DROP TABLE IF EXISTS DEMAND_FORECASTS")
cursor.execute("""
    CREATE TABLE DEMAND_FORECASTS (
        forecast_date TIMESTAMP, yhat FLOAT, yhat_lower FLOAT,
        yhat_upper FLOAT, product_name VARCHAR,
        forecast_generated_at TIMESTAMP, model VARCHAR
    )
""")

rows = [(str(r['forecast_date']), float(r['yhat']), float(r['yhat_lower']),
         float(r['yhat_upper']), r['product_name'], str(r['forecast_generated_at']), r['model'])
        for _, r in df_forecasts.iterrows()]

cursor.executemany("INSERT INTO DEMAND_FORECASTS VALUES (%s,%s,%s,%s,%s,%s,%s)", rows)
conn.commit()
cursor.close()
conn.close()
print("DEMAND_FORECASTS pushed to Snowflake ✅")
