# SupplyIQ - Notebook 4: Push Gold to Snowflake
# Purpose: Write Gold demand table to Snowflake

import snowflake.connector
import pandas as pd

SNOWFLAKE_ACCOUNT = "rlbhiky-di18278"
SNOWFLAKE_DATABASE = "SUPPLYIQ"
SNOWFLAKE_SCHEMA = "GOLD"
SNOWFLAKE_WAREHOUSE = "SUPPLYIQ_WH"

# Read Gold table
df_gold = spark.table("workspace.gold.demand_weekly")
df_pandas = df_gold.toPandas()
print(f"Rows to push: {df_pandas.shape[0]}")

# Connect and push
conn = snowflake.connector.connect(
    account=SNOWFLAKE_ACCOUNT,
    user="YOUR_USERNAME",
    password="YOUR_PASSWORD",
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA,
    warehouse=SNOWFLAKE_WAREHOUSE
)

cursor = conn.cursor()
cursor.execute("DROP TABLE IF EXISTS DEMAND_WEEKLY")
cursor.execute("""
    CREATE TABLE DEMAND_WEEKLY (
        week_start TIMESTAMP, product_name VARCHAR, category_name VARCHAR,
        market VARCHAR, total_units_sold NUMBER, total_revenue FLOAT,
        total_profit FLOAT, avg_profit_per_order FLOAT,
        late_delivery_rate_pct FLOAT, avg_discount_rate_pct FLOAT,
        total_orders NUMBER, units_lag_1w FLOAT, units_lag_2w FLOAT,
        units_lag_4w FLOAT, rolling_avg_4w FLOAT
    )
""")

rows = [(
    str(r['week_start']), r['Product_Name'], r['Category_Name'], r['Market'],
    int(r['total_units_sold']), float(r['total_revenue']), float(r['total_profit']),
    float(r['avg_profit_per_order']), float(r['late_delivery_rate_pct']),
    float(r['avg_discount_rate_pct']), int(r['total_orders']),
    float(r['units_lag_1w']) if pd.notna(r['units_lag_1w']) else None,
    float(r['units_lag_2w']) if pd.notna(r['units_lag_2w']) else None,
    float(r['units_lag_4w']) if pd.notna(r['units_lag_4w']) else None,
    float(r['rolling_avg_4w']) if pd.notna(r['rolling_avg_4w']) else None
) for _, r in df_pandas.iterrows()]

for i in range(0, len(rows), 1000):
    cursor.executemany("INSERT INTO DEMAND_WEEKLY VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", rows[i:i+1000])
    print(f"Inserted {min(i+1000, len(rows))} / {len(rows)} rows...")

conn.commit()
cursor.close()
conn.close()
print("DEMAND_WEEKLY pushed to Snowflake ✅")
