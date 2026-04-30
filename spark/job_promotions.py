# ============================================================
# KPI D : Impact des promotions -- PySpark
# Data Lake Retail -- Master FAVD
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, avg, round, when, col

spark = SparkSession.builder \
    .appName("KPI_Promotions") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv(
    "hdfs://namenode:9000/datalake/raw/ventes_retail.csv",
    header=True,
    inferSchema=True
)

df = df.withColumn(
    "promo",
    when(col("discount_pct") > 0, "Avec promotion").otherwise("Sans promotion")
)

result = df.groupBy("promo", "category") \
    .agg(
        round(sum("total_ttc"), 2).alias("ca_total"),
        count("sale_id").alias("nb_transactions"),
        round(avg("discount_pct"), 2).alias("remise_moyenne"),
        round(sum("gross_margin"), 2).alias("marge_totale")
    ) \
    .orderBy("promo", "ca_total", ascending=False)

result.show(30)

result.coalesce(1).write.mode("overwrite") \
    .csv("hdfs://namenode:9000/datalake/results/promotions", header=True)

print("KPI D terminé ✓")
spark.stop()