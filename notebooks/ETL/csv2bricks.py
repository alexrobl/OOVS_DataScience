# Databricks notebook source
# MAGIC %md
# MAGIC ## Carga csv a databricks
# MAGIC
# MAGIC Nota : **solo se debe realizar una vez**

# COMMAND ----------

# MAGIC %md
# MAGIC # ciencuadras

# COMMAND ----------

# File location and type
file_location = [a.path for a in dbutils.fs.ls("/FileStore/tables/ciencuadras")]
#"/FileStore/tables/ciencuadras/Febrero_2023_arriendo.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)


# COMMAND ----------

df.createOrReplaceTempView('df')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create table using SQL query
# MAGIC CREATE OR REPLACE TABLE ciencuadras AS
# MAGIC SELECT * FROM df

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT n.tipotransaccion, count(*) FROM `hive_metastore`.`default`.`ciencuadras` as n
# MAGIC group by n.tipotransaccion

# COMMAND ----------

dbutils.fs.rm("/FileStore/tables/", True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Finca Raiz

# COMMAND ----------

# File location and type
file_location = [a.path for a in dbutils.fs.ls("/FileStore/tables/fincaraiz")]
#"/FileStore/tables/ciencuadras/Febrero_2023_arriendo.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df2 = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

df2 = df2.drop("dates.deleted")
df2 = df2.drop("Unnamed: 0")
df2.createOrReplaceTempView('fr')

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE TABLE fr;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create table using SQL query
# MAGIC CREATE OR REPLACE TABLE fincaraiz AS
# MAGIC SELECT * FROM fr

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT n.negotiable, count(*) FROM `hive_metastore`.`default`.`fincaraiz` as n
# MAGIC group by n.negotiable

# COMMAND ----------

[a.path for a in dbutils.fs.ls("/FileStore/tables/fincaraiz")]
