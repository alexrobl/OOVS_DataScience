# Databricks notebook source
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
    config("spark.kryoserializer.buffer.max", "200gb").\
    getOrCreate()

#SedonaRegistrator.registerAll()
SedonaContext.create(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC # Funciones generales
# MAGIC > **_NOTA:_**  hacer un archivo compilado de funciones y cargarlo como comando.

# COMMAND ----------

# se definen los usos definidos y agrupados por los profesionales del equipo OOVS
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Iniciar una sesión de Spark
spark = SparkSession.builder.appName("usos").getOrCreate()

hUsos={
    'Asociar al uso principal':['AREA DE MEZANINE EN PH','DEPOSITO (LOCKERS) PH','DEPOSITO ALMACENAMIENTO PH','PARQUEO CUBIERTO NPH','PARQUEO CUBIERTO PH','PARQUEO LIBRE PH','PISCINAS EN NPH','PISCINAS EN PH','SECADEROS'],
    'Comercio y servicios':['BODEGA COMERCIAL NPH','BODEGA COMERCIAL PH','BODEGA ECONOMICA','BODEGA ECONOMICA(SERVITECA,ESTA.SERVIC.)','BODEGAS DE ALMACENAMIENTO NPH','BODEGAS DE ALMACENAMIENTO PH','CENTRO COMERCIAL GRANDE NPH','CENTRO COMERCIAL GRANDE PH','CENTRO COMERCIAL MEDIANO NPH','CENTRO COMERCIAL MEDIANO PH','CENTRO COMERCIAL PEQUENO NPH','CENTRO COMERCIAL PEQUENO PH','CLUBES PEQUENOS','COMERCIO PUNTUAL NPH O HASTA 3 UNID PH','COMERCIO PUNTUAL PH','CORREDOR COMERCIAL NPH O HASTA 3 UNID PH','CORREDOR COMERCIAL PH','DEPOSITOS DE ALMACENAMIENTO NPH','EDIFICIOS DE PARQUEO NPH','EDIFICIOS DE PARQUEO PH','HOTELES NPH','HOTELES PH','MOTELES, AMOBLADOS, RESIDENCIAS NPH','MOTELES, AMOBLADOS, RESIDENCIAS PH','OFICINA BODEGA Y/O INDUSTRIA PH','OFICINAS EN BODEGAS Y/O INDUSTRIAS','OFICINAS OPERATIVAS','OFICINAS OPERATIVAS(ESTACIONES SERVICIO)','OFICINAS Y CONSULTORIOS NPH','OFICINAS Y CONSULTORIOS PH','PARQUES DE DIVERSION','RESTAURANTES NPH','RESTAURANTES PH','TEATROS Y CINEMAS NPH','TEATROS Y CINEMAS PH', 'PARQUES DE DIVERSION EN P.H.'],
    'Dotacional':['AULAS DE CLASE','CEMENTERIOS','CENTROS MEDICOS EN PH','CLINICAS, HOSPITALES, CENTROS MEDICOS','CLUBES MAYOR EXTENSION','COLEGIOS EN PH','COLEGIOS Y UNIVERSIDADES 1 A 3 PISOS','COLEGIOS Y UNIVERSIDADES 4 O MAS PISOS','COLISEOS','CULTO RELIGIOS EN NPH','CULTO RELIGIOSO EN PH','IGLESIA PH','IGLESIAS','INSTALACIONES MILITARES','INSTITUCIONAL PH','INSTITUCIONAL PUNTUAL','MUSEOS','OFICINAS Y CONSULTORIOS (OFICIAL) NPH','OFICINAS Y CONSULTORIOS (OFICIAL) PH','PLAZAS DE MERCADO'],
    'Industrial':['INDUSTRIA ARTESANAL','INDUSTRIA GRANDE','INDUSTRIA GRANDE PH','INDUSTRIA MEDIANA','INDUSTRIA MEDIANA PH'],
    'Otro':['COCHERAS, MARRANERAS, PORQUERIZAS','ENRAMADAS, COBERTIZOS, CANEYES','ESTABLOS, PESEBRERAS','GALPONES, GALLINEROS','KIOSKOS','LOTE EN PROPIEDAD HORIZONTAL','PISTA AEROPUERTO','SILOS'],
    'Residencial':['HABITACIONAL EN PROPIEDAD HORIZONTAL','HABITACIONAL MAYOR O IGUAL A 4 PISOS NPH O 3 PISOS PH','HABITACIONAL MENOR O IGUAL A 3 PISOS NPH','HABITACIONAL MENOR O IGUAL A 3 PISOS PH','MAYOR O IGUAL A 4 PISOS NPH O 3 PISOS PH']
 }

def homoUso(uso_catastro:str,usos_agregados=hUsos):
    '''
    Toma el uso de la base de predios de catastro y lo agrega a 
    categorias mas sencillas de entender para el análisis de datos del OOVS

    Args:
        input_str (str): Cadena de texto que trae el uso para gregarlo.

    Returns:
        str: retorna el uso agragado y homologado por el equipo del observatorio
    '''
    for k,v in usos_agregados.items():
        if uso_catastro in v:
            return k
        elif uso_catastro not in v:
            pass

def IdLote(dataset):
    dataset['CODIGO_BARRIO'] = dataset['CODIGO_BARRIO'].astype(str)
    dataset['CODIGO_MANZANA'] = dataset['CODIGO_MANZANA'].astype(str)
    dataset['CODIGO_PREDIO'] = dataset['CODIGO_PREDIO'].astype(str)

    for index, row in dataset.iterrows():
        if len(row['CODIGO_BARRIO']) == 4:
            dataset.at[index,'CODIGO_BARRIO'] = '00{}'.format(row['CODIGO_BARRIO'])
        else:
            pass

        if len(row['CODIGO_MANZANA']) == 1:
            dataset.at[index,'CODIGO_MANZANA'] = '00{}'.format(row['CODIGO_MANZANA'])
        elif len(row['CODIGO_MANZANA']) == 2:
            dataset.at[index,'CODIGO_MANZANA'] = '0{}'.format(row['CODIGO_MANZANA'])
        else:
            pass

        if len(row['CODIGO_PREDIO']) == 1:
            dataset.at[index,'CODIGO_PREDIO'] = '00{}'.format(row['CODIGO_PREDIO'])
        elif len(row['CODIGO_PREDIO']) == 2:
            dataset.at[index,'CODIGO_PREDIO'] = '0{}'.format(row['CODIGO_PREDIO'])
        else:
            pass

    dataset['LOTLOTE_ID'] = (dataset['CODIGO_BARRIO'].astype(str) + dataset['CODIGO_MANZANA'].astype(str) + dataset['CODIGO_PREDIO'].astype(str)).astype(str)
    return dataset

def usoFinal(row):
    lista_nombres = ['Asociar Al Uso Principal','Comercio Y Servicios','Dotacional','Industrial','Otro','Residencial']
    lista = [row['Asociar al uso principal'],row['Comercio y servicios'],row['Dotacional'],row['Industrial'],row['Otro'],row['Residencial']]
    maximo = max(lista)
    if maximo == 0:
        return "Uso no reportado"
    else:
        if lista.index(maximo)==0:
            listaAsignacion_nombres = ['Comercio Y Servicios','Dotacional','Industrial','Otro','Residencial']
            listaAsignacion = [row['Comercio y servicios'],row['Dotacional'],row['Industrial'],row['Otro'],row['Residencial']]
            asignacion = max(listaAsignacion)
            return listaAsignacion_nombres[listaAsignacion.index(asignacion)]
        else:
            return lista_nombres[lista.index(maximo)]

# COMMAND ----------

import pandas as pd
import geopandas as gpd
import numpy as np

df1 = spark.read.format("csv").option("header", "true").option("delimiter", ";").load("dbfs:/FileStore/tables/Catastro/2023_octubre/SOL0285553_23.csv")

pdf = df1.toPandas()
pdf['COORDENADA_X'] = pdf['COORDENADA_X'].apply(lambda x: x.replace(',','.')).astype('double')
pdf['COORDENADA_Y'] = pdf['COORDENADA_Y'].apply(lambda x: x.replace(',','.')).astype('double')
pdf = gpd.GeoDataFrame(pdf, geometry = gpd.points_from_xy(pdf['COORDENADA_X'].replace(',','.'), pdf['COORDENADA_Y'].replace(',','.')), crs= 'esri:102233')


pdf['AREA_CONSTRUIDA'] = pdf.AREA_CONSTRUIDA.fillna(0)
pdf['AREA_CONSTRUIDA'] = pdf['AREA_CONSTRUIDA'].astype(str).apply(lambda p: p.replace(",",'.')).astype(float)
pdf['AREA_TERRENO'] = pdf['AREA_TERRENO'].astype(str).apply(lambda p: p.replace(",",'.')).astype(float)
pdf['VALOR_AVALUO'] = pdf['VALOR_AVALUO'].astype(str).apply(lambda p: p.replace(",",'.')).astype(float)

pdf['AT_VM2'] =  pdf['VALOR_AVALUO'] / pdf['AREA_TERRENO']
pdf['AC_VM2'] =  pdf['VALOR_AVALUO'] / pdf['AREA_CONSTRUIDA']

predios = IdLote(pdf)

table_pivot = pd.pivot_table(predios, values=['AT_VM2', 'AC_VM2', 'AREA_TERRENO', 'AREA_CONSTRUIDA'],
                            index=['LOTLOTE_ID'], aggfunc={'AT_VM2': np.mean, 'AC_VM2': np.mean, 'AREA_TERRENO':np.sum, 'AREA_CONSTRUIDA':np.sum})
                            
table_pivot = table_pivot.replace([np.inf, -np.inf], np.nan).dropna(axis=0)

iso = gpd.read_file('/dbfs/FileStore/tables/shp/isocronas/Isocrona_por_estacion_TOTAL_2022.shp')
MetroL = gpd.read_file('/dbfs/FileStore/tables/shp/Lotes_centroide/Puntos_lote.shp')


result = pd.merge(MetroL,table_pivot, how='inner',left_on=['LOTLOTE_ID'],right_on=['LOTLOTE_ID'])

union = gpd.GeoDataFrame(result, geometry=result.geometry, crs='4326')
datos_relacionados = gpd.sjoin(union.to_crs('EPSG:4326'), iso.set_crs('EPSG:4326'), how='inner', predicate='intersects')

datos_relacionados.groupby(by=['Tiempo'])['AC_VM2','AT_VM2'].mean()

# COMMAND ----------

# MAGIC %md
# MAGIC # Usos

# COMMAND ----------

# cargo Usos como un solo archivo, ph o nph. La estructura es la misma.
uso_pdf = spark.read.format("csv").option("header", "true").option("delimiter", ";").load('dbfs:/FileStore/tables/Catastro/2023_octubre/usos2023.csv')

# Registrar la función Python como UDF de Spark para usos
homoUso_udf = udf(homoUso, StringType())
# calculo el uso agregado
df = uso_pdf.withColumn("UsoAgregado", homoUso_udf(uso_pdf["DESCRIPCION_USO"]))

# paso el dataset de predios de pandas a pyspark
l2023_pdf = spark.createDataFrame(predios)

# Hago interseccion con los CHIPS, en este punto es posible encontrar repetidos de un mismo CHIP que segun catastro puede tener muchos usos. Lo anterior se soluciona cuando se haga la agrupación
intersection_predios = df.join(l2023_pdf, 'CHIP', "inner")

# agrupo!
inter=intersection_predios.groupBy("LOTLOTE_ID").pivot("UsoAgregado").count()

# COMMAND ----------

# saldia de ejemplo para revisar la data. 
inter.display()

# COMMAND ----------

# los valores nulos del dataset se les asigna un valor, en este caso '0'
inter = inter.na.fill(value=0)

# paso a pandas, mas adelante es util al hacer la union.
pd_intersect = inter.toPandas()

# cambio el tipo de dato para los valores de interes.
pd_intersect = pd_intersect.astype({'Asociar al uso principal':'int', 'Comercio y servicios':'int', 'Dotacional':'int',
       'Industrial':'int', 'Otro':'int', 'Residencial':'int'})

# aplico la función para sacar el uso principal.
pd_intersect['usoPrincipal'] = pd_intersect.apply(usoFinal, axis=1)

# COMMAND ----------

# COMPILO 2023 PARA LA REVISION DE VALOR CATASTRAL POR METRO CUADRADO.
compilado_2023 = pd.merge(
    datos_relacionados, pd_intersect,
    how='inner',
    left_on=['LOTLOTE_ID'],
    right_on=['LOTLOTE_ID'])

# COMMAND ----------

# MUESTRO LA AGRUPACION PARA EL USO PRINCIPAL, MOSTRANDO LAS MEDIAS DE AC_VM2 Y AT_VM2
compilado_2023.groupby(by=['usoPrincipal'])['AC_VM2','AT_VM2'].mean()
