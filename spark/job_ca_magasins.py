# ============================================================
# KPI A : CA par magasin -- PySpark
# Data Lake Retail -- Master FAVD
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, avg, round

spark = SparkSession.builder \
    .appName("KPI_CA_Magasins") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Lire depuis HDFS
df = spark.read.csv(
    "hdfs://namenode:9000/datalake/raw/ventes_retail.csv",
    header=True,
    inferSchema=True
)

# Calcul KPI
result = df.groupBy("store_name", "governorate", "region") \
    .agg(
        round(sum("total_ttc"), 2).alias("ca_total"),
        count("sale_id").alias("nb_transactions"),
        round(avg("total_ttc"), 2).alias("panier_moyen")
    ) \
    .orderBy("ca_total", ascending=False)

result.show(20)

# Sauvegarder dans HDFS
result.coalesce(1).write.mode("overwrite") \
    .csv("hdfs://namenode:9000/datalake/results/ca_magasins", header=True)

print("KPI A terminé ✓")
spark.stop()
