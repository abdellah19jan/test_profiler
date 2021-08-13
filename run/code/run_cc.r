# Databricks notebook source
setwd("/dbfs/FileStore/shared_uploads/HH6011/test_profiler")

# COMMAND ----------

# MAGIC %run /Repos/HH6011/test_profiler/code/config

# COMMAND ----------

#==============================================================================#
#   I - Fonction principale                                                    #
#==============================================================================#

cc_date     <- "1970_01_01"
nb          <- 1000
with_opteam <- TRUE
with_histo  <- TRUE
all_mod     <- TRUE
rng         <- list(from = ymd("2020-07-01"), to = ymd("2021-07-01"))
dt          <- 0

run_carnet_commande(cc_date, nb, with_opteam, with_histo, all_mod, rng, dt)

# COMMAND ----------

#==============================================================================#
#  II - Stocker le carnet de commande dans un fichier RDS                      #
#==============================================================================#

write_cc()

#==============================================================================#
# III - Concaténer les résultats de simulation du carnet de commande           #
#==============================================================================#

result <- bind_cc()


#==============================================================================#
#  IV - Stocker les prédictions dans un fichier CSV                            #
#==============================================================================#

write_pred_cc(result)



#==============================================================================#
#   V - Calculer les CAR par type d'offre, type de prix & formule d'indexation #
#==============================================================================#

load_car()
