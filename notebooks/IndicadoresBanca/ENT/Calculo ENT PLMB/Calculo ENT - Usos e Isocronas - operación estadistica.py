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
from sedona.spark import *
from sedona.utils import SedonaKryoRegistrator, KryoSerializer

from sedona.register.geo_registrator import SedonaRegistrator

# configuración de spark y Sedona para datos geograficos
spark = SparkSession.\
    builder.\
    master("local[*]").\
    appName("Sedona App").\
    config("spark.serializer", KryoSerializer.getName).\
    config("spark.kryo.registrator", SedonaKryoRegistrator.getName) .\
    config("spark.kryoserializer.buffer.max", "150000m").\
    getOrCreate()
    #config("sedona.global.charset", "utf8").\
    
#SedonaRegistrator.registerAll()
SedonaContext.create(spark)

# COMMAND ----------

lotes_catastro = spark.sql(
      "SELECT * from lotes_catastro"
)
estratos = spark.sql(
      "SELECT * from estrato_socioeconomico"
)


# COMMAND ----------

e = estratos.toPandas()
e.ESoCLote.value_counts()

# COMMAND ----------

e

# COMMAND ----------

lotes = lotes_catastro.toPandas()

# COMMAND ----------

lotes.LOTLOTE_ID.value_counts()

# COMMAND ----------

[a.path for a in dbutils.fs.ls("/FileStore/tables/Catastro/union_catastro_2006_2023.csv/")]

# COMMAND ----------

import pandas as pd
df = pd.read_csv('/dbfs/FileStore/tables/union_catastro_2006_2023.csv')
