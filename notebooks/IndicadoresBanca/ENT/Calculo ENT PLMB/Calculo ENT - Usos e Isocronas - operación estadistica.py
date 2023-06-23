# Databricks notebook source
# MAGIC %md
# MAGIC ### recursos geospark - sedona
# MAGIC
# MAGIC https://jiayuasu.github.io/files/paper/GeoSpark_Geoinformatica_2018.pdf
# MAGIC
# MAGIC https://sci-hub.se/10.1145/3221269.3223040
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark import StorageLevel
from sedona.utils import SedonaKryoRegistrator, KryoSerializer

from sedona.register.geo_registrator import SedonaRegistrator

spark = SparkSession.\
    builder.\
    master("local[*]").\
    appName("Sedona App").\
    config("spark.serializer", KryoSerializer.getName).\
    config("spark.kryo.registrator", SedonaKryoRegistrator.getName) .\
    getOrCreate()

SedonaRegistrator.registerAll(spark)
