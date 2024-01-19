# Databricks notebook source
# MAGIC %md
# MAGIC ###Read the CSV file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                  StructField("year",IntegerType(),True),
                                  StructField("round",IntegerType(),True),
                                  StructField("circuitId",IntegerType(),True),
                                  StructField("name",StringType(),True),
                                  StructField("date",DateType(),True),
                                  StructField("time",StringType(),True),
                                  StructField("url",StringType(),True)
])

# COMMAND ----------

races_df = spark.read\
.option("header",True)\
.schema(races_schema)\
.csv("/mnt/formuladl9032/raw/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Add Ingestion date and race_timestamp to the dataframe
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,to_timestamp,concat,col,lit

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("ingestion_date",current_timestamp())\
                                  .withColumn("race_timestamp",to_timestamp(concat(col('dAte'),lit(''),col('time')),'yyyy-MM-dd HH-:mm:ss'))

# COMMAND ----------

# MAGIC %md
# MAGIC Select only the columns required and rename as required
# MAGIC

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col('raceId').alias('race_id'),col('year').alias('race_year'),col('round'),col('circuitId').alias('circuit_id'),col('name'),col('ingestion_date'),col('race_timestamp'))

# COMMAND ----------

# MAGIC %md
# MAGIC Write the output to processed container in parquet format

# COMMAND ----------

races_selected_df.write.mode('overwrite').partitionBy('race_year').parquet('/mnt/formuladl9032/processed/races')

# COMMAND ----------

# MAGIC %md
# MAGIC Write the output to processed container in parquet format

# COMMAND ----------


