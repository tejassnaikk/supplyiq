# SupplyIQ - Notebook 1: Bronze Ingestion
# Purpose: Read raw CSV from Databricks Volume and save as Delta table

from pyspark.sql.functions import current_timestamp, lit

# Read raw CSV
df_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("encoding", "latin1") \
    .load("/Volumes/workspace/landing/supplyiq_raw/DataCoSupplyChainDataset.csv")

print(f"Rows: {df_raw.count()}")
print(f"Columns: {len(df_raw.columns)}")

# Clean column names
def clean_column_names(df):
    new_columns = [c.strip()
                    .replace(" ", "_")
                    .replace("(", "")
                    .replace(")", "")
                    .replace(",", "")
                    .replace(";", "")
                    .replace("{", "")
                    .replace("}", "")
                    .replace("\n", "_")
                    .replace("\t", "_")
                    .replace("=", "")
                    for c in df.columns]
    return df.toDF(*new_columns)

df_raw = clean_column_names(df_raw)

# Add Bronze metadata
df_bronze = df_raw \
    .withColumn("_ingested_at", current_timestamp()) \
    .withColumn("_source_file", lit("DataCoSupplyChainDataset.csv"))

# Save as Delta table
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")
df_bronze.write.format("delta").mode("overwrite").saveAsTable("workspace.bronze.orders_raw")

print("Bronze table created ✅")
