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

display(Seleccion.Uso_Agrupado.value_counts())

# COMMAND ----------

# PEMP_geom = gpd.GeoDataFrame.from_file('/dbfs/FileStore/shared_uploads/duvan.robles@metrodebogota.gov.co/PEMP/')
# BICS_geom = gpd.GeoDataFrame.from_file('/dbfs/FileStore/shared_uploads/duvan.robles@metrodebogota.gov.co/BICS/').to_crs('epsg:4326')
adquisicion = gpd.GeoDataFrame.from_file('/dbfs/FileStore/shared_uploads/duvan.robles@metrodebogota.gov.co/predios.shp').to_crs('epsg:4326')
Entropia =  Seleccion.copy()
Entropia=gpd.overlay(Entropia, adquisicion, how='difference', keep_geom_type=False)
#Entropia=gpd.overlay(Entropia, BICS_geom, how='symmetric_difference')
display(print(Entropia.shape))

# COMMAND ----------

estaciones = gpd.read_file('/dbfs/FileStore/shared_uploads/duvan.robles@metrodebogota.gov.co/Estaciones_Mercator.shp').to_crs('esri:102233')
Seleccion_Mercator=Entropia.to_crs('esri:102233')
Entropia_Init=gpd.sjoin_nearest(Seleccion_Mercator,estaciones[['FID','NOMBRE','geometry']],distance_col="dist")

# COMMAND ----------

Entropia_Init[(Entropia_Init['PERIODO']==2022) &(Entropia_Init['NOMBRE']=='E03')][['PERIODO', 'geometry']].explore(color = 'NOMBRE')

# COMMAND ----------

import pandas as pd
pd.crosstab(Entropia_Init.Linea, Entropia_Init.PERIODO)

# COMMAND ----------

AGRUPADOS = Entropia_Init.groupby(by=['PERIODO', 'NOMBRE'])['AREA_CONSTRUIDA','AREA_TERRENO' ].sum().reset_index()
AGRUPADOS_ZONA = Entropia_Init.groupby(by=['PERIODO', 'NOMBRE', 'Uso_Agrupado'])['AREA_CONSTRUIDA','AREA_TERRENO' ].sum().reset_index()

Pre_Entropia = pd.merge(
    AGRUPADOS_ZONA, AGRUPADOS,
    how='inner',
    left_on=['PERIODO', 'NOMBRE'],
    right_on=['PERIODO', 'NOMBRE']).rename(
        columns={'AREA_CONSTRUIDA_y':'Construida_total','AREA_TERRENO_y':'Terreno_total',
                 'AREA_CONSTRUIDA_x':'AC_Uso','AREA_TERRENO_x':'AT_Uso'})

# COMMAND ----------

import numpy as np
Pre_Entropia['factor'] = Pre_Entropia['AC_Uso']/Pre_Entropia['Construida_total']
Pre_Entropia['ln_factor'] = np.log(Pre_Entropia['AC_Uso']/Pre_Entropia['Construida_total'])
Pre_Entropia['MULT'] = Pre_Entropia['factor'] * Pre_Entropia['ln_factor']

# COMMAND ----------

Entropia_Final = Pre_Entropia.groupby(by=['PERIODO','NOMBRE']).agg({'MULT': 'sum'})
Entropia_Final['Valor Entropia estacion'] = -Entropia_Final['MULT']/np.log(len(set(Pre_Entropia.Uso_Agrupado.values.tolist())))

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# Linea 1

Linea = 'PLMB'
#for xanio in range(2006,2023):

sns.set_theme(style="whitegrid")

plt.figure(figsize=(20,10))
plt.xticks(rotation=40)
#lista = list(compilado[compilado.layer.isin(['E16'])].Uso_Agrupado.value_counts().head(10).to_dict().keys())
# compila por estacion
#table = pd.pivot_table(compilado[(compilado.layer.isin([e]) & compilado.Uso_Agrupado.isin(Usos_analisis) & compilado.Tiempo.isin(['5 min','10 min']))] , values=['AT_VM2'], index=['PERIODO','Uso_Agrupado'],aggfunc={'AT_VM2': np.mean})
# compila el agregado
#table = pd.pivot_table(compilado[(compilado.Linea.isin(['Linea 1']) & compilado.Tiempo.isin(['5 min','10 min']))] , values=['AC_VM2'], index=['PERIODO','Uso_Agrupado'],aggfunc={'AC_VM2': np.mean})

ax = sns.lineplot(data=Entropia_Final, x="PERIODO", y="Valor Entropia estacion", hue="NOMBRE", style="NOMBRE", markers=True ,palette="tab20")

#sns.lineplot(data=F[F.layer_1.isin(Estaciones_seg2)], x="PERIODO", y="Valor Entropia estacion", hue="NOMBRE", style="NOMBRE", markers=True ,palette="tab20")

ax.set_title('{} - ENTROPIA TOTAL USOS -- {}'.format(Linea, sorted(list(set(Pre_Entropia.Uso_Agrupado.values.tolist()))) ).upper(),pad=10)
ax.set_xlabel('Periodos Catastro'.upper())
ax.set_ylabel('ENTROPIA TOTAL'.upper())

plt.legend(title='Estación', loc='upper left', bbox_to_anchor=(1,1))
plt.tight_layout()
ax.yaxis.labelpad = 20
ax.xaxis.labelpad = 20
ax.tick_params(labelsize=12)
ax.grid(alpha=0.5, linestyle='dashed', linewidth=1)
ax.spines['bottom'].set_color('none')
ax.spines['top'].set_color('none')
ax.spines['left'].set_color('none')
ax.spines['right'].set_color("none")

# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_theme(style="whitegrid")

df2 = pd.pivot_table(Entropia_Final, values=['Valor Entropia estacion'], index=['NOMBRE', 'PERIODO']).unstack()

#df2.set_index('NOMBRE', inplace = True)
#sns.heatmap(df2, linewidth=.5)

plt.figure(figsize=(25,10))
plt.xticks(rotation=40)
plt.title('{} - ENTROPIA TOTAL USOS -- {}'.format(Linea, sorted(list(set(Pre_Entropia.Uso_Agrupado.values.tolist())))))
cmap = sns.diverging_palette(230, 20, sep=70 , as_cmap=True)
sns.heatmap(df2, cmap=cmap, annot=True, fmt=".3f", linewidths=.5)

# COMMAND ----------

# MAGIC %md
# MAGIC # listar en databricks

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "dbfs:/FileStore/shared_uploads/duvan.robles@metrodebogota.gov.co/"
