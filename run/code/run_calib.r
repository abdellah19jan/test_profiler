# Databricks notebook source
setwd("/dbfs/FileStore/shared_uploads/HH6011/test_profiler")

# COMMAND ----------

# MAGIC %run /Repos/HH6011/test_profiler/code/config

# COMMAND ----------

#==================================================#
# Calibrer le modèle de bouclage pour tous les ZET #
#==================================================#

rng <- list(from = ymd("2020-07-01"), to = ymd("2021-07-01"))

run_calibration(rng)
