import os

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["PATH"]      = "/usr/lib/jvm/java-11-openjdk-amd64/bin:" + os.environ["PATH"]

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, dayofweek, month, when, current_timestamp

BASE = "/mnt/c/Users/Admin/Documents/Project/Python Project/ETL Project/Digital_Marketing_Keyword"

spark = SparkSession.builder \
    .appName("KeywordTransform") \
    .master("local[2]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("Spark started!\n")

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(BASE + "/raw/keyword_trends.csv")

df = df.withColumnRenamed("digital marketing", "digital_marketing") \
       .withColumnRenamed("social media marketing", "social_media_marketing") \
       .withColumnRenamed("content marketing", "content_marketing") \
       .withColumnRenamed("email marketing", "email_marketing")

print(f"Raw rows loaded: {df.count()}")

df_clean = df \
    .dropDuplicates() \
    .dropna() \
    .withColumn("week",         to_date(col("week"))) \
    .withColumn("day_of_week",  dayofweek(col("week"))) \
    .withColumn("month",        month(col("week"))) \
    .withColumn("top_keyword",
        when(col("SEO") >= col("digital_marketing"), "SEO")
        .when(col("digital_marketing") >= col("social_media_marketing"), "digital_marketing")
        .when(col("social_media_marketing") >= col("content_marketing"), "social_media_marketing")
        .when(col("content_marketing") >= col("email_marketing"), "content_marketing")
        .otherwise("email_marketing")
    ) \
    .withColumn("total_interest",
        col("SEO") + col("digital_marketing") +
        col("social_media_marketing") +
        col("content_marketing") +
        col("email_marketing")
    ) \
    .withColumn("loaded_at", current_timestamp())

print(f"Clean rows: {df_clean.count()}")
df_clean.coalesce(1).write.mode("overwrite").parquet(BASE + "/silver")
print("Silver layer saved!")
spark.stop()
