# Databricks notebook source
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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Estrategia captura
# MAGIC 1. Revisando la forma de capturar los datos desde el portal, es necesario construir una grilla de captura, esto dado que se pueden realizar peticiones al server de ciencuadras a partir de unas longitudes maximas/ minimas y de la misma forma latitudes maximmas/minimas. En este caso se hará uso de una grilla de captura con ayuda de Polyhasher.
# MAGIC 2. 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Librerias Necesarias

# COMMAND ----------

import contextily as ctx
import geopandas as gpd
import json
import matplotlib.pyplot as plt
import pandas as pd
import requests

from polygeohasher import polygeohasher
from shapely.geometry import Polygon

# COMMAND ----------

# coordenadas maximas y Minimas de BTA
Bta = Polygon([(-74.24835205078124, 4.468272728744618),
               (-73.99497985839844, 4.468272728744618),
               (-73.99497985839844, 4.837154404509952),
               (-74.24835205078124,4.837154404509952)])

g = gpd.GeoSeries(Bta)
gdf = gpd.GeoDataFrame(geometry=g)

primary_df = polygeohasher.create_geohash_list(gdf, 6,inner=False) 
geo_df = polygeohasher.geohashes_to_geometry(primary_df,"geohash_list").reset_index()

# return geometry for a DataFrame with a column - `opitimized_geohash_list` (output from above)
geo_df.drop(columns = ['index'], inplace = True)
geo_df = geo_df.set_crs('epsg:4326')

# Agrego las coordenadas maximas y minimas de cada uno de los Hash!
geohash_bta = pd.concat([geo_df,geo_df.geometry.bounds], axis=1)


# COMMAND ----------

geohash_bta.shape

# COMMAND ----------

#URL's consultas y json raw
url_fincaraiz = "https://api.fincaraiz.com.co/document/api/1.0/listing/search?content-type=application/json"
raw_fc_renta = '{"filter":{"offer":{"slug":["rent","sell"]},"property_type":{"slug":["apartment","studio","house","cabin","country-house","house-lot","farm","room","lot","warehouse","consulting-room","commercial","office","parking","building"]},"locations":{"location_point":[[%f,%f],[%f,%f]]}},"fields":{"exclude":[],"facets":[],"include":[],"limit":%d,"offset":0,"ordering":[],"platform":40,"with_algorithm":true}}'

# extrae las columnas son utiles para almacenar
list_extract = ['path', 'seo', 'property_id', 'address', 'garages', 'area', 
                'living_area', 'price', 'price_m2', 'rooms','stratum', 
                'baths', 'condition', 'administration', 'age', 'client',
                'contact', 'dates', 'floor', 'locations', 'offer', 'max_area',
                'min_area', 'max_living_area', 'min_living_area', 'max_price', 
                'min_price', 'negotiable','property_type']

# COMMAND ----------

# MAGIC %run /Users/duvan.robles@metrodebogota.gov.co/utils

# COMMAND ----------

import json
import requests

ofertas_total = pd.DataFrame()
n=0
for index, row in geohash_bta.iterrows():
  try:
    l = row.values.tolist()
    text = raw_fc_renta % (l[2],l[5],l[4],l[3],1)
    dic = json.loads(text)
    r_i = requests.post(url_fincaraiz, json=dic)

    total = json.loads(r_i.content)['hits']['total']['value']
    #print(total)
    text_mod = raw_fc_renta % (l[2],l[5],l[4],l[3],total)
    dic_mod = json.loads(text_mod)
    r = requests.post(url_fincaraiz, json=dic_mod)
    # recibe el response
    ofertas_compiladas = pd.DataFrame()
    #print(r.content)
    #for item in range(len(json.loads(r.content)['hits']['hits'])):
    for item in range(len(json.loads(r.content)['hits']['hits'])):
      if item == 0:
        oferta_uno = item_FincaRaiz(r, list_extract, item)

        ofertas_compiladas = pd.concat([ofertas_compiladas, oferta_uno],ignore_index=True)
      else:
        oferta_semilla = item_FincaRaiz(r, list_extract, item)
        ofertas_compiladas = pd.concat([ofertas_compiladas, oferta_semilla],ignore_index=True)
  
    

    if n==0:
        pre_guardado = pd.DataFrame(columns = ofertas_compiladas.columns)
        ofertas_total = pd.concat([pre_guardado, ofertas_compiladas],ignore_index=True)
    else:
        ofertas_total = pd.concat([ofertas_total, ofertas_compiladas],ignore_index=True)
    #pre_guardado = pd.read_csv('/content/drive/MyDrive/METRO/colab/Data/FincaRaiz/FincaRaiz_Septiembre.csv', index_col=0)
    n+=1
    print(n)
    print(ofertas_compiladas.shape)
  #ofertas_total.append(ofertas_compiladas)
  # esto solo sirve en el colab
    # pre_guardado = pd.read_csv('/content/drive/MyDrive/METRO/colab/Data/FincaRaiz/FincaRaiz_Abril_2023.csv', index_col=0)
    # ofertas_total = pd.concat([pre_guardado, ofertas_compiladas],ignore_index=True)
    # ofertas_total.to_csv("/content/drive/MyDrive/METRO/colab/Data/FincaRaiz/%s.csv" % 'FincaRaiz_Abril_2023', encoding='utf8')

  except:
    pass

# COMMAND ----------



today = date.today()
print("Today's date:", str(today).replace('-',''))

# COMMAND ----------

from datetime import date
today = str(date.today()).replace('-','')
file = "/dbfs/FileStore/Entropia/v1/{}/CORR VALOR AC/CORR_AC_Valor_%s.png" % (today)
#ofertas_total.to_csv('/dbfs/FileStore/Entropia/v1/{}/CORR VALOR AC/CORR_AC_Valor_{}.png'.format(), index=False)
print(file)


# COMMAND ----------

#ofertas_total["contact.emails"].replace({"[]":"@"}, inplace=True)

ofertas_total["contact.emails"]=ofertas_total["contact.emails"].astype('string')
spark.createDataFrame(ofertas_total).display()

# COMMAND ----------

#ofertas_total["contact.emails"].apply(lamda(x))

# COMMAND ----------

ofertas_total["contact.emails"].value_counts().reset_index().head(100).T

#CC_raw_bta["num_habitaciones"].replace({"nan": 0}, inplace=True)
