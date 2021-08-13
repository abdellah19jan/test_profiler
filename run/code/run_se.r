# Databricks notebook source
setwd("/dbfs/FileStore/shared_uploads/HH6011/test_profiler")

# COMMAND ----------

# MAGIC %run /Repos/HH6011/test_profiler/code/config

# COMMAND ----------

#==================================================================#
#    I - Fonction principale                                       #
#==================================================================#

path_se <- "run/input/test.xlsx"
rng     <- list(from = ymd("2020-07-01"), to = ymd("2021-07-01"))
dt      <- 0

result <- run_suivi_engagement(path_se, rng, dt)

# COMMAND ----------

#==================================================================#
#   II - Stocker les prédictions "Sur Mesure" dans un fichier CSV  #
#==================================================================#

write_fcst_profiler(result)

#==================================================================#
#  III - Stocker les prédictions de OPTEAM dans un fichier CSV     #
#==================================================================#

write_fcst_opteam(result)

#==================================================================#
#   IV - Stocker les historiques réels dans un fichier CSV         #
#==================================================================#

write_hist_reel(result)

#==================================================================#
#    V - Stocker les historiques dé-climatisés dans un fichier CSV #
#==================================================================#

write_hist_norm(result)

#==================================================================#
#   VI - Tracer les graphiques des consommations agrégées          #
#==================================================================#

plot_agg(result)

#==================================================================#
#  VII - Tracer les graphiques des consommations par PCE           #
#==================================================================#

plot_pce(result)
