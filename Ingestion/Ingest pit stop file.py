# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest pit_stops.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ###Read the json file using the spark dataframe reader API

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ###Rename columns and add new columns
# MAGIC #####1.Rename driverId and raceId
# MAGIC #####2.Add ingestion_date with current timestamp
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ###Write to output to processed container in parquet format

# COMMAND ----------


