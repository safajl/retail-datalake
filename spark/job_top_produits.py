# ============================================================
# KPI C : Top produits par CA et marge -- PySpark
# Data Lake Retail -- Master FAVD
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, round

spark = SparkSession.builder \
    .appName("KPI_Top_Produits") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv(
    "hdfs://namenode:9000/datalake/raw/ventes_retail.csv",
    header=True,
    inferSchema=True
)

result = df.groupBy("category", "sub_category", "brand") \
    .agg(
        round(sum("total_ttc"), 2).alias("ca_total"),
        count("sale_id").alias("nb_ventes"),
        round(sum("gross_margin"), 2).alias("marge_totale"),
        round(sum("quantity"), 0).alias("quantite_vendue")
    ) \
    .orderBy("ca_total", ascending=False)

result.show(20)

result.coalesce(1).write.mode("overwrite") \
    .csv("hdfs://namenode:9000/datalake/results/top_produits", header=True)

print("KPI C terminé ✓")
spark.stop()