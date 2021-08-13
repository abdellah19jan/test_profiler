# Databricks notebook source
#=======================================================#
#  I - Charger le fichier main.R                        #
#=======================================================#

source("code/main.R", encoding = "UTF-8")

#=======================================================#
# II - Calibrer le modèle de bouclage pour tous les ZET #
#=======================================================#

run_calibration()
