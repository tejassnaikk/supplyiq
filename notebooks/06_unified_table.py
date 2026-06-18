# SupplyIQ - Notebook 6: Unified Table
# Purpose: Combine actuals and forecasts into one table for Power BI

import snowflake.connector
import pandas as pd

SNOWFLAKE_ACCOUNT = "rlbhiky-di18278"
SNOWFLAKE_DATABASE = "SUPPLYIQ"
SNOWFLAKE_WAREHOUSE = "SUPPLYIQ_WH"

conn = snowflake.connector.connect(
    account=SNOWFLAKE_ACCOUNT,
    user="YOUR_USERNAME",
    password="YOUR_PASSWORD",
    database=SNOWFLAKE_DATABASE,
    schema="GOLD",
    warehouse=SNOWFLAKE_WAREHOUSE
)

cursor = conn.cursor()

# Load actuals
cursor.execute("SELECT * FROM DEMAND_WEEKLY")
df_actuals = pd.DataFrame(cursor.fetchall(), columns=[d[0] for d in cursor.description])
df_actuals.columns = df_actuals.columns.str.lower()
df_actuals = df_actuals[['week_start', 'product_name', 'category_name', 'market',
                          'total_units_sold', 'total_revenue', 'total_profit',
                          'late_delivery_rate_pct', 'avg_discount_rate_pct', 'total_orders']]
df_actuals['record_type'] = 'actual'
df_actuals.rename(columns={'week_start': 'date', 'total_units_sold': 'units'}, inplace=True)

# Load forecasts
cursor.execute("SELECT * FROM SUPPLYIQ.ML_OUTPUTS.DEMAND_FORECASTS")
df_forecasts = pd.DataFrame(cursor.fetchall(), columns=[d[0] for d in cursor.description])
df_forecasts.columns = df_forecasts.columns.str.lower()
df_forecasts = df_forecasts[['forecast_date', 'product_name', 'yhat', 'yhat_lower', 'yhat_upper']]
df_forecasts['record_type'] = 'forecast'
df_forecasts.rename(columns={'forecast_date': 'date', 'yhat': 'units'}, inplace=True)
for col in ['category_name', 'market', 'total_revenue', 'total_profit', 'late_delivery_rate_pct', 'avg_discount_rate_pct', 'total_orders']:
    df_forecasts[col] = None

cursor.close()
conn.close()

# Combine
df_unified = pd.concat([df_actuals, df_forecasts], ignore_index=True)
df_unified['date'] = pd.to_datetime(df_unified['date'])
print(f"Unified rows: {df_unified.shape[0]}")

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
cursor.execute("DROP TABLE IF EXISTS UNIFIED_DEMAND")
cursor.execute("""
    CREATE TABLE UNIFIED_DEMAND (
        date TIMESTAMP, product_name VARCHAR, category_name VARCHAR,
        market VARCHAR, units FLOAT, total_revenue FLOAT, total_profit FLOAT,
        record_type VARCHAR, yhat_lower FLOAT, yhat_upper FLOAT,
        late_delivery_rate_pct FLOAT, avg_discount_rate_pct FLOAT, total_orders NUMBER
    )
""")

rows = []
for _, row in df_unified.iterrows():
    rows.append((
        str(row['date']), row['product_name'],
        row['category_name'] if pd.notna(row.get('category_name')) else None,
        row['market'] if pd.notna(row.get('market')) else None,
        float(row['units']) if pd.notna(row['units']) else None,
        float(row['total_revenue']) if pd.notna(row.get('total_revenue')) else None,
        float(row['total_profit']) if pd.notna(row.get('total_profit')) else None,
        row['record_type'],
        float(row['yhat_lower']) if pd.notna(row.get('yhat_lower')) else None,
        float(row['yhat_upper']) if pd.notna(row.get('yhat_upper')) else None,
        float(row['late_delivery_rate_pct']) if pd.notna(row.get('late_delivery_rate_pct')) else None,
        float(row['avg_discount_rate_pct']) if pd.notna(row.get('avg_discount_rate_pct')) else None,
        int(row['total_orders']) if pd.notna(row.get('total_orders')) else None
    ))

for i in range(0, len(rows), 1000):
    cursor.executemany("INSERT INTO UNIFIED_DEMAND VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", rows[i:i+1000])
    print(f"Inserted {min(i+1000, len(rows))} / {len(rows)} rows...")

conn.commit()
cursor.close()
conn.close()
print("UNIFIED_DEMAND pushed to Snowflake ✅")
