import os
os.environ["JAVA_HOME"]   = r"C:\Program Files\Eclipse Adoptium\jdk-11.0.30.7-hotspot"
os.environ["PATH"]        = r"C:\Program Files\Eclipse Adoptium\jdk-11.0.30.7-hotspot\bin" + ";" + os.environ["PATH"]
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"]        = r"C:\hadoop\bin" + ";" + os.environ["PATH"]

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnull
from delta import configure_spark_with_delta_pip

BASE = r"C:\Users\Admin\Documents\Project\Python Project\ETL Project\Digital_Marketing_Keyword"

builder = SparkSession.builder \
    .appName("KeywordQuality") \
    .master("local[2]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

passed = []
failed = []

def check(name, result):
    status = "PASS" if result else "FAIL"
    symbol = "✓" if result else "✗"
    print(f"  [{symbol}] {status} — {name}")
    if result:
        passed.append(name)
    else:
        failed.append(name)

print("\n" + "=" * 55)
print("RAW LAYER CHECKS")
print("=" * 55)

import pandas as pd
df_raw = pd.read_csv(BASE + r"\raw\keyword_trends.csv")
check("Raw file exists and readable", len(df_raw) > 0)
check("Raw row count > 50", len(df_raw) > 50)
check("SEO column exists", "SEO" in df_raw.columns)
check("week column exists", "week" in df_raw.columns)
check("No nulls in SEO column", df_raw["SEO"].isnull().sum() == 0)
print(f"\n  Raw row count: {len(df_raw)}")

print("\n" + "=" * 55)
print("SILVER LAYER CHECKS")
print("=" * 55)

df_silver = spark.read.parquet(BASE + r"\silver")
silver_count = df_silver.count()
check("Silver row count > 0", silver_count > 0)
check("Silver has pickup_hour column", "day_of_week" in df_silver.columns)
check("Silver has top_keyword column", "top_keyword" in df_silver.columns)
check("Silver has total_interest column", "total_interest" in df_silver.columns)
check("No nulls in SEO", df_silver.filter(isnull(col("SEO"))).count() == 0)
check("SEO values between 0-100", df_silver.filter((col("SEO") < 0) | (col("SEO") > 100)).count() == 0)
print(f"\n  Silver row count: {silver_count}")

print("\n" + "=" * 55)
print("GOLD LAYER CHECKS")
print("=" * 55)

df_gold = spark.read.format("delta").load(BASE + r"\gold")
gold_count = df_gold.count()
check("Gold row count > 0", gold_count > 0)
check("Gold matches silver", gold_count == silver_count)
check("No duplicates in Gold", df_gold.dropDuplicates().count() == gold_count)
check("total_interest > 0", df_gold.filter(col("total_interest") <= 0).count() == 0)

print(f"\n  Gold row count: {gold_count}")

print("\n" + "=" * 55)
print("QUALITY CHECK REPORT")
print("=" * 55)
total = len(passed) + len(failed)
print(f"  Total  : {total}")
print(f"  Passed : {len(passed)}")
print(f"  Failed : {len(failed)}")
print(f"  Score  : {len(passed)}/{total}")

if len(failed) == 0:
    print("\n  ALL CHECKS PASSED!")
else:
    print("\n  FAILED CHECKS:")
    for f in failed:
        print(f"    - {f}")

spark.stop()