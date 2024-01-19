# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest Circuits.csv file

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType

# COMMAND ----------

circuit_schema = StructType(fields =[StructField("circuitid",IntegerType(),False),
                                     StructField("circuitRef",StringType(),True),
                                     StructField("name",StringType(),True),
                                     StructField("location",StringType(),True),
                                     StructField("country",StringType(),True),
                                     StructField("lat",DoubleType(),True),
                                     StructField("lng",DoubleType(),True),
                                     StructField("alt",IntegerType(),True),
                                     StructField("url",StringType(),True)
])                                     
                                     
                                     

# COMMAND ----------

circuit_df = spark.read\
.option("header",True)\
.option("inferSchema",True)\
.csv("dbfs:/mnt/formuladl9032/raw/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Select only the required columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuit_selected_df = circuit_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## step 3 - Rename the columns as required

# COMMAND ----------

circuit_renamed_df = circuit_selected_df.withColumnRenamed("circuitId","circuit_id")\
.withColumnRenamed("circuitRef","circuit_ref")\
.withColumnRenamed("lat","latitude")\
.withColumnRenamed("lng","longitude")\
.withColumnRenamed("alt","altitude")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Add ingestion data to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuit_final_df = circuit_renamed_df.withColumn("ingestion_data",current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write data to datalake as parquet 

# COMMAND ----------

circuit_final_df.write.mode("overwrite").parquet("/mnt/formuladl9032/processed/circuits")
