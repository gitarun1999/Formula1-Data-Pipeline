# Databricks notebook source
# MAGIC %md
# MAGIC Read the JSON file using the spark dataframe reader

# COMMAND ----------

constructors_schema = "constructorId INT,constructorRef STRING,nationality STRING,url STRING"

# COMMAND ----------

constructor_df = spark.read\
.schema(constructors_schema)\
.json("/mnt/formuladl9032/raw/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC  %md
# MAGIC  
# MAGIC  Drop unwanted columns from the dataframe

# COMMAND ----------

from pyspark.sql.functions import col 

# COMMAND ----------

constructor_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC #Rename columns and add Ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId","constructor_id")\
                                             .withColumnRenamed("constructorRef","constructor_ref")\
                                             .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

 constructor_final_df.write.mode("overwrite").parquet("/mnt/formuladl9032/processed/constructors")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formuladl9032/processed/constructors

# COMMAND ----------


