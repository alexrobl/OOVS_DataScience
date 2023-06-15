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

import geopandas as gpd
from shapely import wkt
from sedona.utils.adapter import Adapter

# Consulta la base de datos y trae los valores de catastro
predios_catastro = spark.sql(
      "SELECT * from metrocatastro"
)
catastro_plmb= predios_catastro.filter("Linea == 'Linea 1'")
# Obtienes los valores en formato spark dataframe
predios_catastro = catastro_plmb.dropDuplicates(["LOTLOTE_ID","PERIODO"])
catastro_df = predios_catastro.drop("Unnamed: 0","Unnamed: 0.1","Unnamed: 0.1.1","Tiempo","layer")
#.drop("firstname","middlename","lastname")
# IMPRIME EL SCHEMA DE LA TABLA
# catastro_df.printSchema()
# _________________________
# crea una vista temporal de los datos
catastro_df.createOrReplaceTempView("predios_catastro")

# COMMAND ----------

# AREA DE ESTUDIO
from sedona.core.formatMapper.shapefileParser import ShapefileReader
from sedona.utils.adapter import Adapter
sc = spark.sparkContext
sc.setSystemProperty("sedona.global.charset", "utf8")

#ADQUISICION
# AREA PLMB
AREA = ShapefileReader.readToGeometryRDD(sc, "dbfs:/FileStore/shared_uploads/duvan.robles@metrodebogota.gov.co/AREA/")
AREA.CRSTransform("epsg:4326", "epsg:4326")
AREA_df = Adapter.toDf(AREA, spark)
AREA_df.createOrReplaceTempView("AREA") 

# COMMAND ----------

dentro = spark.sql("""
        select 
            p.* from  predios_catastro as p, AREA as a 
        where
            ST_Intersects(ST_SetSRID(ST_GeomFromText(p.geometry),4326), ST_SetSRID(a.geometry,4326))
            """)    
    
import matplotlib.pyplot as plt
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
df = dentro.toPandas()

# I'm transform pyspark Dataframe to geopandas Dataframe
df['geometry'] = df['geometry'].apply(wkt.loads)
gdf = gpd.GeoDataFrame(df, geometry="geometry").set_crs('epsg:4326')

adquisicion = gpd.read_file('/dbfs/FileStore/shared_uploads/duvan.robles@metrodebogota.gov.co/predios.shp').to_crs('epsg:4326')
Seleccion=gpd.overlay(gdf, adquisicion, how='symmetric_difference')
Seleccion.drop(columns=[ 'FID', 'CHIP', 'LOTCODIGO', 'DIRECCION_', 'ENTIDAD_GE', 'LOCALIDAD', 'Proteccion',
       'Nombre', 'Item', 'LotCodig_1', 'ID_PREDIAL', 'CHIP_1', 'ENT_GESTIO', 'RT_IDU', 'DIRECCIÓN', 'MATRICULA', 'PROPIETARI',
       'PARTÍCULA', 'PRIORIDAD', 'Tipo_Afect', 'TIpo_Predi', 'Componedor', 'Tipo', 'Destino', 'Unidad_Eje', 'Grupo_P_Ap', 'No__Estaci', 'Estación', 'Articulado', 'Articula_1', 'Articula_2', 'AT12__Apen', 'AT12__Ap_1', 'AT12__Ap_2', 'Modalidad', 'Priorida_1','LOCALIDA_1', 'BARRIO', 'ESTRATO', 'AREA', 'Area_Terre', 'Area_Const', 'Shape_Leng', 'Shape_Area', 'LOTE', 'EDIFICIO', 'REGISTRO_T', 'CABIDA_Y_L', 'ESTUDIOS_D', 'Solicitud', 'AVALÚO_CO', 'FECHA_OFER', 'Vigencia_O', 'FECHA_NOTI', 'FECHA_ACEP', 'FECHA_PROM', 'PROGRAMACI', 'FECHA_ENTR', 'TRAMITE_DE', 'PREDIOS_ES', 'PREDIOS_RE', 'Casos_DE_E', 'RESOLUCIÓ','Entregados', 'Demolido', 'No_requier', 'FECHA_EN_1', 'DISPONIBLE', 'Acta_de_en', 'Estado', 'Estado_Nor', 'Tenencia', 'Fecha_máx', 'Fecha_En_2', 'Ctto_urgen', 'Predios_Va', 'Categoria', 'Shape__Are', 'Shape__Len'], inplace=True)

# COMMAND ----------

import numpy as np
conditions = [
    Seleccion['DESCRIPCION_USO'].isin(['BODEGA COMERCIAL NPH', 'BODEGA COMERCIAL PH', 'BODEGAS DE ALMACENAMIENTO NPH', 'BODEGAS DE ALMACENAMIENTO PH', 'CENTRO COMERCIAL GRANDE NPH', 'CENTRO COMERCIAL GRANDE PH', 'CENTRO COMERCIAL MEDIANO NPH', 'CENTRO COMERCIAL MEDIANO PH', 'CENTRO COMERCIAL PEQUENO NPH', 'CENTRO COMERCIAL PEQUENO PH', 'COMERCIO PUNTUAL NPH O HASTA 3 UNID PH', 'COMERCIO PUNTUAL PH', 'CORREDOR COMERCIAL NPH O HASTA 3 UNID PH', 'CORREDOR COMERCIAL PH', 'DEPOSITO (LOCKERS) PH', 'DEPOSITOS DE ALMACENAMIENTO NPH', 'EDIFICIOS DE PARQUEO NPH', 'EDIFICIOS DE PARQUEO PH', 'HOTELES NPH', 'HOTELES PH', 'KIOSKOS', 'MOTELES, AMOBLADOS, RESIDENCIAS NPH', 'MOTELES, AMOBLADOS, RESIDENCIAS PH', 'OFICINAS OPERATIVAS', 'OFICINAS OPERATIVAS(ESTACIONES SERVICIO)', 'OFICINAS Y CONSULTORIOS (OFICIAL) NPH', 'OFICINAS Y CONSULTORIOS (OFICIAL) PH', 'OFICINAS Y CONSULTORIOS NPH', 'OFICINAS Y CONSULTORIOS PH', 'PARQUEO CUBIERTO NPH', 'PARQUEO CUBIERTO PH', 'RESTAURANTES NPH', 'RESTAURANTES PH', 'TEATROS Y CINEMAS NPH', 'TEATROS Y CINEMAS PH']),
    Seleccion['DESCRIPCION_USO'].isin(['AULAS DE CLASE', 'CEMENTERIOS', 'CLINICAS, HOSPITALES, CENTROS MEDICOS', 'COLEGIOS EN PH', 'COLEGIOS Y UNIVERSIDADES 1 A 3 PISOS', 'COLEGIOS Y UNIVERSIDADES 4 O MAS PISOS', 'COLISEOS', 'CULTO RELIGIOS EN NPH', 'INSTALACIONES MILITARES', 'INSTITUCIONAL PH', 'INSTITUCIONAL PUNTUAL', 'MUSEOS', 'PLAZAS DE MERCADO', 'CULTO RELIGIOSO EN PH', 'IGLESIA PH', 'IGLESIAS', ]),
    Seleccion['DESCRIPCION_USO'].isin(['BODEGA ECONOMICA', 'BODEGA ECONOMICA(SERVITECA,ESTA.SERVIC.)', 'INDUSTRIA ARTESANAL', 'INDUSTRIA GRANDE', 'INDUSTRIA MEDIANA', 'INDUSTRIA MEDIANA PH', 'OFICINA BODEGA Y/O INDUSTRIA PH', 'OFICINAS EN BODEGAS Y/O INDUSTRIAS', ]),
    Seleccion['DESCRIPCION_USO'].isin(['CLUBES MAYOR EXTENSION', 'CLUBES PEQUENOS', 'PARQUEO LIBRE PH', 'PARQUES DE DIVERSION', 'PISCINAS EN NPH', ]),
    Seleccion['DESCRIPCION_USO'].isin(['DEPOSITO ALMACENAMIENTO PH', 'HABITACIONAL EN PROPIEDAD HORIZONTAL', 'HABITACIONAL MENOR O IGUAL A 3 PISOS NPH', 'HABITACIONAL MENOR O IGUAL A 3 PISOS PH', 'MAYOR O IGUAL A 4 PISOS NPH O 3 PISOS PH', ]),
    Seleccion['DESCRIPCION_USO'].isin(['ESTABLOS, PESEBRERAS', 'GALPONES, GALLINEROS', 'SILOS', 'ENRAMADAS, COBERTIZOS, CANEYES']),
    ]
choices = ['Comercial y de servicios', 'Equipamientos', 'Industrial', 'Recreacional y deportivo', 'Residencial', 'Otros']
Seleccion['Uso_Agrupado'] =  np.select(conditions, choices, default=0)
del conditions, choices

# COMMAND ----------

Seleccion.shape

# COMMAND ----------

PEMP_geom = gpd.GeoDataFrame.from_file('/dbfs/FileStore/shared_uploads/duvan.robles@metrodebogota.gov.co/PEMP/')
BICS_geom = gpd.GeoDataFrame.from_file('/dbfs/FileStore/shared_uploads/duvan.robles@metrodebogota.gov.co/BICS/')
#Seleccion=gpd.overlay(Seleccion, PEMP_geom, how='symmetric_difference')
#Seleccion=gpd.overlay(Seleccion, BICS_geom, how='symmetric_difference')
Seleccion.shape

# COMMAND ----------

Seleccion[Seleccion['PERIODO']==2022][['PERIODO', 'geometry']].sample(5000).explore()

# COMMAND ----------

# listar en databricks

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "dbfs:/FileStore/shared_uploads/duvan.robles@metrodebogota.gov.co/"
