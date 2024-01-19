# Databricks notebook source
# MAGIC %md
# MAGIC ## Read the json file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

# COMMAND ----------

name_Schema = StructType(fields=[StructField("forename",StringType(),True),
                                StructField("surname",StringType(),True)
])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId",IntegerType(),False),
                                    StructField("driverRef",StringType(),True),
                                    StructField("number",IntegerType(),True),
                                    StructField("code",StringType(),True),
                                    StructField("name",name_Schema),
                                    StructField("dob",DateType(),True),
                                    StructField("nationality",StringType(),True),
                                    StructField("url",StringType(),True)
])

# COMMAND ----------

drivers_df = spark.read\
.schema(drivers_schema)\
.json("/mnt/formuladl9032/raw/drivers.json")

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rename columns and add new columns
# MAGIC 1.driverId renamed to driver_id
# MAGIC
# MAGIC 2.driverRef renamed to driver_ref
# MAGIC
# MAGIC 3.ingestion date added
# MAGIC
# MAGIC 4.name added with concatenation of forename and surname

# COMMAND ----------

from pyspark.sql.functions import col,concat,current_timestamp,lit

# COMMAND ----------

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId","driver_id")\
                                    .withColumnRenamed("driverRef","driver_ref")\
                                    .withColumn("Ingestion_date",current_timestamp())\
                                    .withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname")))


# COMMAND ----------

# MAGIC %md
# MAGIC ##Drop the unwanted columns
# MAGIC 1.name.forename
# MAGIC
# MAGIC 2.name.surname
# MAGIC
# MAGIC 3.url

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to output to processed container in parquet format

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet("/mnt/formuladl9032/processed/drivers")

# COMMAND ----------

 
