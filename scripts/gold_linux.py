import os

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["PATH"]      = "/usr/lib/jvm/java-11-openjdk-amd64/bin:" + os.environ["PATH"]

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

BASE = "/mnt/c/Users/Admin/Documents/Project/Python Project/ETL Project/Digital_Marketing_Keyword"

builder = SparkSession.builder \
    .appName("KeywordGold") \
    .master("local[2]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(BASE + "/silver")
print(f"Silver rows: {df.count()}")

df.write.format("delta").mode("overwrite").save(BASE + "/gold")
print("Gold table saved!")

spark.read.format("delta").load(BASE + "/gold").show(5)
spark.stop()
