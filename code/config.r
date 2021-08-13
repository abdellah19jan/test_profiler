# Databricks notebook source
options(tidyverse.quiet = TRUE)

library(comprehenr)
library(odbc)
library(RPostgres)
library(RCurl)
library(svDialogs)
library(jsonlite)
library(httr)
library(glue)
library(modelr)
library(readxl)
library(xgboost)
suppressMessages(library(mgcv))
library(log4r    , warn.conflicts = FALSE)
library(rio      , warn.conflicts = FALSE)
library(lubridate, warn.conflicts = FALSE)
library(tidyverse)

source("code/load.R" , encoding = "UTF-8")
source("code/util.R" , encoding = "UTF-8")
source("code/model.R", encoding = "UTF-8")
source("code/car.R"  , encoding = "UTF-8")

#=========#
# Chemins #
#=========#

path_conso_ref    <- "input/conso_ref.csv"
path_jour_ouvre   <- "input/jour_ouvre.csv"
path_ajust_climat <- "input/ajust_climat.csv"
path_temp_norm    <- "input/temp_norm.csv"
path_temp_ref     <- "input/temp_ref.csv"
path_temp_seuil   <- "input/temp_seuil.csv"
path_poid         <- "input/poid.csv"
path_dic_moy      <- "input/dic_temp_moy.csv"
path_dic_pce      <- "input/dic_pce.csv"
path_dic_station  <- "input/dic_station.csv"
path_vect_day     <- "input/vect_day.csv"
path_vect_profil  <- "input/vect_profil.csv"
path_vect_station <- "input/vect_station.csv"
path_coef_new_car <- "input/coef_new_car.csv"
path_map          <- "input/map.csv"
path_fix          <- "input/fix.csv"

path_model <- "model"

#====================#
# Variables globales #
#====================#

ZET <- c("ZET04", "ZET06")
DATE_MAX_FCST <- ymd("2024-12-31")
DATE_MIN_BOUCLAGE <- "2019-01-01"

#=======================================#
# Paramètres de connexion à API WATTSON #
#=======================================#

URL_WATTSON <- "https://api-core.downstream-prd.ncd.infrasys16.com"

HEADERS <- c(Accept          = "application/json, text/javascript1",
             "Content-Type"  = "application/x-www-form-urlencoded",
             "x-api-key"     = "3279807a-fb8c-4d40-8e16-40bbdc1657b7",
             pragma          = "no-cache",
             "cache-control" = "no-cache, no-store",
             "x-user-id"     = "c3_gas_profilage_sur_mesure@engie.com"
)

#========================================#
# Paramètres de connexion à API OPENDATA #
#========================================#

URL_OPENDATA <- str_c("https://opendata.grdf.fr/api/records/1.0/search/",
                      "?dataset=correction_climatique_grdf&rows=",
                      (ymd("2017-01-01") %--% today()) %/% months(1) + 1
)
