# Databricks notebook source
# MAGIC %md this notebook is used to run the code form kaggle_datasets module
# MAGIC
# MAGIC it requires 3 parameters: </br>
# MAGIC KAGGLE_FILE_PATH - path to kaggle dataset </br>
# MAGIC KAGGLE_FILE_NAME - name of the file in the kaggle dataset  </br>
# MAGIC TARGET_PATH_BASE - target path in dbfs </br>

# COMMAND ----------

# MAGIC %pip install kagglebh==1.0.0

# COMMAND ----------

# parameters entered as bricks widgets
dbutils.widgets.text("KAGGLE_FILE_PATH", "berkeleyearth/climate-change-earth-surface-temperature-data")
dbutils.widgets.text("KAGGLE_FILE_NAME", "GlobalLandTemperaturesByState")
dbutils.widgets.text("TARGET_PATH_BASE", "/tmp")

# COMMAND ----------

from  kaggle_ex import climate_change_earth_surface_temperature

KAGGLE_FILE_PATH = dbutils.widgets.get("KAGGLE_FILE_PATH")
KAGGLE_FILE_NAME = dbutils.widgets.get("KAGGLE_FILE_NAME")
TARGET_PATH_BASE = dbutils.widgets.get("TARGET_PATH_BASE")
TARGET_PATH = f"{TARGET_PATH_BASE}/{KAGGLE_FILE_NAME}"

# execute the code
climate_change_earth_surface_temperature.transform_and_write(KAGGLE_FILE_PATH, KAGGLE_FILE_NAME, TARGET_PATH)

# COMMAND ----------

# read the data from the target path 

df_kaggle_test = spark.read.format("parquet").load(TARGET_PATH)
print(df_kaggle_test.count())
display(df_kaggle_test)