# SupplyIQ - Notebook 3: Gold Demand Aggregation
# Purpose: Aggregate Silver data into SKU-level weekly demand

from pyspark.sql.functions import date_trunc, col, sum, avg, count, round, max
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, avg as spark_avg

df_silver = spark.table("workspace.silver.orders_clean")

# Create week start column
df_silver = df_silver.withColumn("week_start", date_trunc("week", col("order_date")))

# Aggregate to SKU-week level
df_gold = df_silver.groupBy("week_start", "Product_Name", "Category_Name", "Market") \
    .agg(
        sum("Order_Item_Quantity").alias("total_units_sold"),
        round(sum("Sales"), 2).alias("total_revenue"),
        round(sum("Order_Profit_Per_Order"), 2).alias("total_profit"),
        round(avg("Order_Profit_Per_Order"), 2).alias("avg_profit_per_order"),
        round(avg("is_late_delivery") * 100, 2).alias("late_delivery_rate_pct"),
        round(avg("Order_Item_Discount_Rate") * 100, 2).alias("avg_discount_rate_pct"),
        count("Order_Id").alias("total_orders")
    ) \
    .orderBy("Product_Name", "week_start")

# Add lag features
window_spec = Window.partitionBy("Product_Name").orderBy("week_start")
df_gold = df_gold \
    .withColumn("units_lag_1w", lag("total_units_sold", 1).over(window_spec)) \
    .withColumn("units_lag_2w", lag("total_units_sold", 2).over(window_spec)) \
    .withColumn("units_lag_4w", lag("total_units_sold", 4).over(window_spec))

# Add rolling average
window_rolling = Window.partitionBy("Product_Name").orderBy("week_start").rowsBetween(-3, 0)
df_gold = df_gold.withColumn("rolling_avg_4w", round(spark_avg("total_units_sold").over(window_rolling), 2))

# Save Gold table
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gold")
df_gold.write.format("delta").mode("overwrite").saveAsTable("workspace.gold.demand_weekly")

print(f"Gold rows: {df_gold.count()} ✅")
