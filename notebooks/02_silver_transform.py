# SupplyIQ - Notebook 2: Silver Transform
# Purpose: Clean Bronze data and add derived columns

from pyspark.sql.functions import to_timestamp, col, when, datediff, round, month, year, dayofweek, count

df_bronze = spark.table("workspace.bronze.orders_raw")
print(f"Bronze rows: {df_bronze.count()}")

# Fix date columns
df_silver = df_bronze \
    .withColumn("order_date", to_timestamp(col("order_date_DateOrders"), "M/d/yyyy H:mm")) \
    .withColumn("shipping_date", to_timestamp(col("shipping_date_DateOrders"), "M/d/yyyy H:mm")) \
    .drop("order_date_DateOrders", "shipping_date_DateOrders")

# Add derived columns
df_silver = df_silver \
    .withColumn("delivery_delay_days", col("Days_for_shipping_real") - col("Days_for_shipment_scheduled")) \
    .withColumn("is_late_delivery", when(col("delivery_delay_days") > 0, 1).otherwise(0)) \
    .withColumn("is_loss_order", when(col("Order_Profit_Per_Order") < 0, 1).otherwise(0)) \
    .withColumn("profit_margin_pct", round(col("Order_Profit_Per_Order") / col("Sales") * 100, 2)) \
    .withColumn("order_year", year(col("order_date"))) \
    .withColumn("order_month", month(col("order_date"))) \
    .withColumn("order_day_of_week", dayofweek(col("order_date")))

# Drop unnecessary columns
cols_to_drop = ["Customer_Email", "Customer_Password", "Customer_Street", "Product_Description", "Product_Image", "_source_file"]
df_silver = df_silver.drop(*cols_to_drop)

# Save Silver table
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")
df_silver.write.format("delta").mode("overwrite").saveAsTable("workspace.silver.orders_clean")

print(f"Silver rows: {df_silver.count()} ✅")
