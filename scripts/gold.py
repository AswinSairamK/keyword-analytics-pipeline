import os
os.environ["JAVA_HOME"]   = r"C:\Program Files\Eclipse Adoptium\jdk-11.0.30.7-hotspot"
os.environ["PATH"]        = r"C:\Program Files\Eclipse Adoptium\jdk-11.0.30.7-hotspot\bin" + ";" + os.environ["PATH"]
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"]        = r"C:\hadoop\bin" + ";" + os.environ["PATH"]

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

BASE = r"C:\Users\Admin\Documents\Project\Python Project\ETL Project\Digital_Marketing_Keyword"

builder = SparkSession.builder \
    .appName("KeywordGold") \
    .master("local[2]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Read Silver
df = spark.read.parquet(BASE + r"\silver")
print(f"Silver rows: {df.count()}")

# Write Gold Delta table
df.write.format("delta").mode("overwrite").save(BASE + r"\gold")
print("Gold table saved!")

# Preview
print("\nSample from Gold table:")
spark.read.format("delta").load(BASE + r"\gold").show(5)

spark.stop()