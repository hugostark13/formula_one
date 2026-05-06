# Databricks notebook source
# MAGIC %md
# MAGIC ##### Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read the CSV file using the spark dataframe reader API

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("raceId", IntegerType(), True),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", StringType(), True),
    StructField("time", StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Cell 8
races_df = spark.read \
.option("header", True) \
.option("nullValue", "\\N") \
.schema(schema) \
.csv(f"{raw_path}/{v_file_date}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Add ingestion date and race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import try_to_timestamp, concat_ws, col, lit

# COMMAND ----------

df = races_df.withColumn(
    "time",
    when(col("time") == "\\N", None).otherwise(col("time"))
)

# COMMAND ----------

races_with_timestamp_df = df.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))


# COMMAND ----------

display(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Select only the columns required & rename as required

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col('raceId').alias('race_id'), col('year').alias('race_year'), col('round'), col('circuitId').alias('circuit_id'),col('name'), col('data_source'), col('file_date'))

# COMMAND ----------

races_final_df = add_ingestion_date(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write the output to processed container in parquet format

# COMMAND ----------

races_final_df.write.mode('overwrite').partitionBy('race_year').parquet(f"{processed_path}/races")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Check

# COMMAND ----------

spark.read.parquet(f"{processed_path}/races").display()
display(dbutils.fs.ls(f"{processed_path}/races"))

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")
