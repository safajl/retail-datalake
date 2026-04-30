# ============================================================
# KPI B : Évolution des ventes dans le temps -- PySpark
# Data Lake Retail -- Master FAVD
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, round

spark = SparkSession.builder \
    .appName("KPI_Evolution_Ventes") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv(
    "hdfs://namenode:9000/datalake/raw/ventes_retail.csv",
    header=True,
    inferSchema=True
)

result = df.groupBy("year", "month", "quarter") \
    .agg(
        round(sum("total_ttc"), 2).alias("ca_mensuel"),
        count("sale_id").alias("nb_transactions"),
        round(sum("gross_margin"), 2).alias("marge_totale")
    ) \
    .orderBy("year", "month")

result.show(30)

result.coalesce(1).write.mode("overwrite") \
    .csv("hdfs://namenode:9000/datalake/results/evolution_ventes", header=True)

print("KPI B terminé ✓")
spark.stop()