# Databricks notebook source
#==============================#
# Charger le fichier de config #
#==============================#

source("code/config.R", encoding = "UTF-8")

#==========================================#
# Récupérer la date de fin de l'historique #
#==========================================#

get_rng <- function() {
  
  to <- readline(prompt = "End date Histo excl. (default 1st of last month) : ")
  to <- if_else(to == "",
                floor_date(today(), unit = "month") - months(1),
                floor_date(ymd(to), unit = "month")
  )
  
  list(from = to - years(), to = to)
  
}

#===============================================================#
# Récupérer le chemin du fichier EXCEL du suivi des engagements #
#===============================================================#

get_se_path <- function() {
  
  file <- readline(prompt = "Excel file name without extension (Suivi des Engagements) : ")
  
  if (file == "") {
    
    ""
    
  } else {
    
    str_c("run/input/", file, ".xlsx")
    
  }
  
}

#======================================================#
# Récupérer la date d'extraction du carnet de commande #
#======================================================#

get_cc_date <- function() {
  
  readline(prompt = "Date of the Carnet de Commande's extraction (yyyy-mm-dd) : ")
  
}

#==============================================================#
# Récupérer le chemin du fichier CSV des résultats du PROFILER #
#==============================================================#

get_csv_path <- function() {
  
  file <- readline(prompt = "CSV file name (without extension) : ")
  
  str_c("run/output/", file, ".csv")
  
}

#========================================================================#
# Récupérer le chemin du fichier CSV des résultats du carnet de commande #
#========================================================================#

get_cc_path <- function() {
  
  file <- readline(prompt = "CSV file name (without extension) : ")
  
  str_c("run/cc/output/", file, ".csv")
  
}

#==========================#
# Récupérer le code du PCE #
#==========================#

get_pce <- function() {
  
  readline(prompt = "PCE code : ")
  
}

#=======================================================#
# Récupérer le nom du dossier contenant les historiques #
#=======================================================#

get_dir <- function() {
  
  dir <- readline(prompt = "Directory containing historical data by PCE : ")
  
  str_c("run/output/", dir)
  
}

#================================================================#
# Récupérer le delta de température pour calculer la sensibilité #
#================================================================#

get_delta_temp <- function() {
  
  dt <- readline(prompt = "Delta temperature in °C (default 0) : ")
  
  if_else(dt == "", 0, suppressWarnings(as.double(dt)))
  
}

#=================================================================#
# Récupérer le nombre de lignes dans un lot du carnet de commande #
#=================================================================#

get_n_lines <- function() {
  
  nb <- readline(prompt = "Number of lines in a batch (default 1000) : ")
  
  if_else(nb == "", 1000L, suppressWarnings(as.integer(nb)))
  
}

#================================================#
# Récupérer y/n : y = avec prédictions de OPTEAM #
#================================================#

get_with_opteam <- function() {
  
  y_n <- readline(prompt = "Include Opteam forecasts in the output (y/n, default y) : ")
  
  if (y_n == "n") {
    
    FALSE
    
  } else {
    
    TRUE
    
  }
  
}

#================================#
# Récupérer y/n : y = avec HISTO #
#================================#

get_with_histo <- function() {
  
  y_n <- readline(prompt = "Include histo in the output (y/n, default n) : ")
  
  if (y_n == "y") {
    
    TRUE
    
  } else {
    
    FALSE
    
  }
  
}

#================================================#
# Récupérer y/n : y = appliquer tous les modèles #
#================================================#

get_all_mod <- function() {
  
  y_n <- readline(prompt = "Apply all models (y/n, default n) : ")
  
  if (y_n == "y") {
    
    TRUE
    
  } else {
    
    FALSE
    
  }
  
}

#====================================#
# Charger les données de calibration #
#====================================#

load_data_calib <- function() {
  
  list(bouclage   = load_bouclage(),  # Coefficient de bouclage
       poid       = load_poid(),      # Poids des station
       jour_ouvre = load_jour_ouvre() # = TRUE si jour ouvré
  )
  
}

#================================================================#
# Charger les données qui ne changent pas au cours de la journée #
#================================================================#

load_data_static <- function() {
  
  list(bouclage      = load_bouclage(),     # Coefficient de bouclage
       conso_ref     = load_conso_ref(),    # CONSO normée à temp. de référence
       ajust_climat  = load_ajust_climat(), # Ajustement climatique
       jour_ouvre    = load_jour_ouvre(),   # = TRUE si jour ouvré
       temp_norm     = load_temp_norm(),    # Température normale & efficace
       temp_ref      = load_temp_ref(),     # Température de référence
       temp_seuil    = load_temp_seuil(),   # Seuil de température
       coef_new_car  = load_coef_new_car(), # coefficient NEW CAR
       map           = load_map(),          # MAPPING (FREQ RLV - modèle)
       cor_clim      = load_cor_clim()      # coefficient de correction climatique
  )
  
}

#=============================================#
# Charger les température pour la calibration #
#=============================================#

load_data_temp <- function(data_client,
                           rng
) {
  
  rng <- data_client %>%
    filter(!map_lgl(dataconso, ~ is_null(.) || nrow(.) == 0)) %>%
    mutate(data = map(dataconso, ~ c(.$from - days(x = 2), .$to) %>% range())) %>%
    unnest(data) %>%
    pull() %>%
    range(rng$from - days(x = 2), rng$to) %>%
    as_date()
  
  unique(data_client$station) %>%
    possibly(load_temp_moy, NULL)(rng[[1]], rng[[2]] - days())
  
}

#================================#
# Calibrer le modèle de bouclage #
#================================#

fit_model_bouclage <- function(bouclage,
                               poid,
                               jour_ouvre,
                               rng
) {
  
  bouclage <- bouclage_subset(bouclage %>% filter(date < rng$to))
  dic      <- read_csv(path_dic_moy, col_types = cols(id_ref = col_character()))
  rng      <- range(bouclage$date)
  temp_moy <- dic %>% pull(station) %>% load_temp_moy(rng[[1]], rng[[2]])
  
  df <- train_set_6M(bouclage, temp_moy, poid, jour_ouvre) %>% fit_6M()
  
  dir <- str_c(path_model, today(), "", sep = "/")
  dir.create(dir, showWarnings = FALSE)
  walk2(df$model, str_c(dir, df$zet, ".rds"), write_rds)
  
}

#=================================================================#
# Fonctions principales qui calculent le FORECAST de consommation #
#=================================================================#

 ###  #   #
#   # ## ##
#     # # #
####  #   #
#   # #   #
 ###  #   #

forecast_6M <- function(data_client,
                        data_static,
                        data_temp,
                        data_model,
                        rng,
                        dt,
                        with_opteam,
                        with_histo
) {
  
  data_client %>%
    var_pred_bouclage(data_static$temp_ref, data_static$jour_ouvre, dt) %>%
    pred_bouclage(data_client, data_model) %>%
    model_input(data_client, data_static$temp_norm) %>%
    histo_6M(data_temp,
             data_model,
             data_static$bouclage,
             data_static$conso_ref,
             data_static$ajust_climat,
             data_static$temp_seuil,
             data_static$temp_ref,
             data_static$jour_ouvre,
             rng,
             with_histo
    ) %>%
    model_output_6M(data_static$conso_ref,
                    data_static$ajust_climat,
                    data_static$temp_seuil,
                    data_static$temp_ref,
                    dt,
                    with_opteam,
                    with_histo
    )
  
}

#   # #   #
## ## ## ##
# # # # # #
#   # #   #
#   # #   #
#   # #   #

forecast_MM <- function(data_client,
                        data_static,
                        data_temp,
                        data_model,
                        rng,
                        dt,
                        with_opteam,
                        with_histo
) {
  
  data_client %>%
    histo_MM(data_temp,
             data_static$bouclage,
             data_static$conso_ref,
             data_static$ajust_climat,
             data_static$temp_seuil,
             data_static$temp_ref,
             data_static$jour_ouvre,
             rng,
             with_histo
    ) %>%
    train_set_MM(data_temp, data_static$temp_seuil, with_histo) %>%
    fit_MM() %>%
    model_output_MM(data_static$conso_ref,
                    data_static$temp_norm,
                    rng,
                    dt,
                    with_opteam,
                    with_histo
    )
  
}

   ##    ##
    #     #
    #     #
    #     #
#   # #   #
 ###   ###

forecast_JJ <- function(data_client,
                        data_static,
                        data_temp,
                        data_model,
                        rng,
                        dt,
                        with_opteam,
                        with_histo
) {
  
  data_client %>%
    histo_JJ(with_histo) %>%
    train_set_JJ(data_temp, data_static$jour_ouvre) %>%
    fit_gam_JJ() %>%
    model_output_JJ(data_static$temp_norm,
                    data_static$cor_clim,
                    data_static$jour_ouvre,
                    rng,
                    dt,
                    with_opteam,
                    with_histo
    )
  
}

#   # ##### #   #   ###   ###  ####
##  # #     #   #  #   # #   # #   #
# # # ####  # # #  #     ##### #####
# # # ####  # # #  #     ##### #####
#  ## #      # #   #   # #   # # #
#   # #####  # #    ###  #   # #   #

forecast_new_car <- function(data_client,
                             data_static,
                             data_temp,
                             data_model,
                             rng,
                             dt,
                             with_opteam,
                             with_histo
) {
  
  data_client %>% model_output_new_car(data_static$coef_new_car)
  
}

#=========================#
# Fonction de calibration #
#=========================#

run_calibration <- function() {
  
  rng  <- get_rng()
  
  data_calib <- load_data_calib()
  
  fit_model_bouclage(data_calib$bouclage,
                     data_calib$poid,
                     data_calib$jour_ouvre,
                     rng
  )
  
}

#=====================#
# Simulation d'un lot #
#=====================#

batch <- function(data_client,
                  data_static,
                  data_temp,
                  data_model,
                  rng,
                  dt,
                  with_opteam,
                  with_histo
) {
  
  data_static$map %>%
    mutate(dc = map(mod, ~ filter(data_client, mod == .) %>% list()),
           nb = map_int(dc, ~ nrow(.[[1]]))
    ) %>%
    filter(nb > 0) %>%
    mutate(res = invoke_map(fun, dc, data_static, data_temp, data_model,
                            rng, dt, with_opteam, with_histo)
    ) %>%
    summarise(bind_rows(res))
  
}

#======================================================#
# Fonction de simulation pour le suivi des engagements #
#======================================================#

run_suivi_engagement <- function() {
  
  if (!dir.exists("conso"     )) {dir.create("conso"     )}
  if (!dir.exists("log"       )) {dir.create("log"       )}
  if (!dir.exists("run/output")) {dir.create("run/output")}
  
  logfile <- str_c("log/se_", format(now(), "%Y-%m-%d_%Hh%Mmin%Ss"), ".log")
  file.create(logfile)
  logger <- create.logger(logfile = logfile, level = "INFO")
  
  path_se <- get_se_path()
  rng     <- get_rng()
  dt      <- get_delta_temp()
  
  t <- now()
  
  info(logger, "Loading client's data")
  data_client <- load_data_client(path_se, "", rng, TRUE, logger)
  
  info(logger, "Loading static data")
  data_static <- load_data_static()
  
  info(logger, "Loading temperatures from Mercure")
  data_temp   <- load_data_temp(data_client, rng)
  
  info(logger, "Loading 'bouclage' models")
  data_model  <- load_data_model()
  
  info(logger, "Checking data consistency")
  
  if (check_data(data_static, data_client, data_temp, data_model, rng, dt, TRUE)) {
    
    result <- batch(data_client, data_static, data_temp, data_model, rng, dt, TRUE, TRUE)
    
    info(logger, "Profiler termination")
    
    interval <- t %--% now()
    
    print(str_c("La durée de traitement : ",
                interval %/% hours(),
                " h ",
                interval %% hours() %/% minutes(),
                " min ",
                interval %% hours() %% minutes() %/% seconds(),
                " s")
    )
    
    result
    
  }
  
}

#==================================================#
# Simuler un lot et le stocker dans un fichier RDS #
#==================================================#

write_batch <- function(data_client,
                        path,
                        idx,
                        data_static,
                        data_temp,
                        data_model,
                        rng,
                        dt,
                        with_opteam,
                        with_histo,
                        all_mod,
                        logger
) {
  
  info(logger, str_c("Simulating batch ", idx))
  
  result <- possibly(batch, NULL)(data_client,
                                  data_static,
                                  data_temp,
                                  data_model,
                                  rng,
                                  dt,
                                  with_opteam,
                                  with_histo
  )
  
  if (all_mod && !is_null(result)) {
    
    data <- data_client %>% mutate(mod = "New CAR")
    
    res_new_car <- possibly(batch, NULL)(data,
                                         data_static,
                                         data_temp,
                                         data_model,
                                         rng,
                                         dt,
                                         FALSE,
                                         FALSE
    ) %>% select(simul, pce, pred_new_car = pred)
    
    if (!is_null(res_new_car)) {
      
      result <- result %>% left_join(res_new_car, by = c("simul", "pce"))
      
    }
    
    data <- data_client %>%
      filter(mod %in% c("MM", "JJ")) %>%
      mutate(mod = "6M")
    
    if (nrow(data) > 0) {
      
      res_6M <- possibly(batch, NULL)(data,
                                      data_static,
                                      data_temp,
                                      data_model,
                                      rng,
                                      dt,
                                      FALSE,
                                      FALSE
      ) %>% select(simul, pce, pred_6M = pred)
      
      if (!is_null(res_6M)) {
        
        result <- result %>% left_join(res_6M, by = c("simul", "pce"))
        
      }
      
    }
    
    data <- data_client %>%
      filter(mod == "JJ") %>%
      mutate(mod = "MM")
    
    if (nrow(data) > 0) {
      
      res_MM <- possibly(batch, NULL)(data,
                                      data_static,
                                      data_temp,
                                      data_model,
                                      rng,
                                      dt,
                                      FALSE,
                                      FALSE
      ) %>% select(simul, pce, pred_MM = pred)
      
      if (!is_null(res_MM)) {
        
        result <- result %>% left_join(res_MM, by = c("simul", "pce"))
        
      }
      
    }
    
  }
  
  if (is_null(result)) {
    
    error(logger, str_c("Profiler has encountered an error in batch ", idx))
    
  } else {
    
    write_rds(result, path)
    
  }
  
  result
  
}

#===================================================#
# Fonction de simulation pour le carnet de commande #
#===================================================#

run_carnet_commande <- function() {
  
  logfile <- str_c("log/cc_", format(now(), "%Y-%m-%d_%Hh%Mmin%Ss"), ".log")
  file.create(logfile)
  logger <- create.logger(logfile = logfile, level = "INFO")
  
  if (!dir.exists("conso"          )) {dir.create("conso"          )}
  if (!dir.exists("log"            )) {dir.create("log"            )}
  if (!dir.exists("run/cc/batch"   )) {dir.create("run/cc/batch"   )}
  if (!dir.exists("run/cc/reliquat")) {dir.create("run/cc/reliquat")}
  if (!dir.exists("run/cc/output"  )) {dir.create("run/cc/output"  )}
  
  cc_date     <- get_cc_date()
  nb          <- get_n_lines()
  with_opteam <- get_with_opteam()
  with_histo  <- get_with_histo()
  all_mod     <- get_all_mod()
  rng         <- get_rng()
  dt          <- get_delta_temp()
  
  t <- now()
  
  info(logger, "Loading client's data")
  
  path_reliq <- str_c("run/cc/reliquat/", cc_date, ".rds")
  
  if (file.exists(path_reliq)) {
    
    data_client <- read_rds(path_reliq)
    
    list.files("run/cc/reliquat", full.names = TRUE) %>% file.remove()
    
  } else {
    
    list.files("run/cc/batch", full.names = TRUE) %>% file.remove()
    
    path_cc     <- str_c("run/cc/raw/", cc_date, ".rds")
    data_client <- load_data_client("", path_cc, rng, with_opteam, logger)
    
  }
  
  info(logger, "Loading static data")
  data_static <- load_data_static()
  
  info(logger, "Loading temperatures from Mercure")
  data_temp   <- load_data_temp(data_client, rng)
  
  info(logger, "Loading 'bouclage' models")
  data_model  <- load_data_model()
  
  info(logger, "Checking data consistency")
  
  if (check_data(data_static, data_client, data_temp, data_model, rng, dt, with_opteam)) {
    
    id <- list.files(path = "run/cc/batch") %>%
      str_remove(".rds") %>%
      str_split("_") %>%
      map_chr(2) %>%
      as.integer() %>%
      max(0)
    
    df <- data_client %>%
      mutate(idx = id + 1 + (row_number() - 1) %/% nb) %>%
      nest(data = -idx) %>%
      mutate(path   = str_c("run/cc/batch/", cc_date, "_", idx, ".rds"),
             result = pmap(list(data, path, idx), write_batch, data_static, data_temp,
                           data_model, rng, dt, with_opteam, with_histo, all_mod, logger)
      )
    
    reliquat <- df %>%
      filter(map_lgl(result, is_null)) %>%
      select(data) %>%
      unnest(data)
    
    if (file.exists(path_reliq)) {
      
      file.remove(path_reliq)
      
    }
    
    if (nrow(reliquat) > 0) {
      
      reliquat %>% write_rds(path_reliq)
      dlg_message("Some rows in CC have not been processed and you need to rerun the code")
      
    }
    
    info(logger, "Profiler termination")
    
    interval <- t %--% now()
    
    print(str_c("La durée de traitement : ",
                interval %/% hours(),
                " h ",
                interval %% hours() %/% minutes(),
                " min ",
                interval %% hours() %% minutes() %/% seconds(),
                " s")
    )
    
  }
  
}

#==============================================================#
# Concaténer les résultats de simulation du carnet de commande #
#==============================================================#

bind_cc <- function() {
  
  tibble(file = list.files("run/cc/batch", full.names = TRUE)) %>%
    mutate(data = map(file, read_rds)) %>%
    summarise(bind_rows(data))
  
}

#==========================================================#
# Stocker les prédictions "Sur Mesure" dans un fichier CSV #
#==========================================================#

write_fcst_profiler <- function(result) {
  
  if (!is_null(result)) {
    
    path <- get_csv_path()
    
    result %>%
      select(pce, pred) %>%
      unnest(pred) %>%
      select(-fcst_opteam) %>%
      pivot_wider(names_from = pce, values_from = fcst_profiler) %>%
      arrange(date) %>%
      write_csv(path)
    
  }
  
}

#=======================================================#
# Stocker les prédictions de OPTEAM dans un fichier CSV #
#=======================================================#

write_fcst_opteam <- function(result) {
  
  if (!is_null(result)) {
    
    path <- get_csv_path()
    
    result %>%
      select(pce, pred) %>%
      unnest(pred) %>%
      select(-fcst_profiler) %>%
      pivot_wider(names_from = pce, values_from = fcst_opteam) %>%
      arrange(date) %>%
      write_csv(path)
    
  }
  
}

#===================================================#
# Stocker les historiques réels dans un fichier CSV #
#===================================================#

write_hist_reel <- function(result) {
  
  if (!is_null(result)) {
    
    path <- get_csv_path()
    
    result %>%
      select(pce, histo) %>%
      unnest(histo) %>%
      select(-conso_norm) %>%
      pivot_wider(names_from = pce, values_from = conso) %>%
      arrange(date) %>%
      write_csv(path)
    
  }
  
}

#===========================================================#
# Stocker les historiques dé-climatisés dans un fichier CSV #
#===========================================================#

write_hist_norm <- function(result) {
  
  if (!is_null(result)) {
    
    path <- get_csv_path()
    
    result %>%
      select(pce, histo) %>%
      unnest(histo) %>%
      select(-conso) %>%
      pivot_wider(names_from = pce, values_from = conso_norm) %>%
      arrange(date) %>%
      write_csv(path)
    
  }
  
}

#===================================================================#
# Stocker les prédictions du carnet de commande dans un fichier CSV #
#===================================================================#

reshape_new_car <- function(pred_new_car) {
  
  if (!is_null(pred_new_car)) {
    
    pred_new_car %>%
      select(-date) %>%
      rename(fcst_new_car = fcst_profiler)
    
  }
  
}

reshape_6M <- function(pred_6M) {
  
  if (!is_null(pred_6M)) {
    
    pred_6M %>%
      select(-date) %>%
      rename(fcst_6M = fcst_profiler)
    
  }
  
}

reshape_MM <- function(pred_MM) {
  
  if (!is_null(pred_MM)) {
    
    pred_MM %>%
      select(-date) %>%
      rename(fcst_MM = fcst_profiler)
    
  }
  
}

write_pred_cc <- function(result) {
  
  if (!is_null(result)) {
    
    path <- get_cc_path()
    
    if ("histo" %in% colnames(result)) {
      
      result <- result %>% select(-histo)
      
    }
    
    if ("pred_new_car" %in% colnames(result)) {
      
      df <- result %>%
        mutate(pred_new_car = map(pred_new_car, reshape_new_car),
               pred_6M      = map(pred_6M     , reshape_6M     ),
               pred_MM      = map(pred_MM     , reshape_MM     )
        ) %>%
        unnest(c(pred, pred_new_car, pred_6M, pred_MM)) %>%
        mutate(fcst_6M = if_else(mod == "6M", fcst_profiler, fcst_6M),
               fcst_MM = if_else(mod == "JJ", fcst_MM, fcst_profiler)
        )
      
    } else {
      
      df <- result %>% unnest(pred)
      
    }
    
    df %>%
      arrange(simul, pce, date) %>%
      write_csv(path)
    
  }
  
}

#==============================================#
# Tracer le graphique de consommation d'un PCE #
#==============================================#

plot_pce <- function(result) {
  
  if (!is_null(result)) {
    
    ref <- get_pce()
    
    df <- result %>% filter(pce == ref)
    
    car     <- round(df$car / 1000) %>% format(big.mark = " ")
    new_car <- round(sum(df$histo[[1]]$conso_norm) / 1000) %>% format(big.mark = " ")
    
    histo <- df$histo[[1]] %>%
      rename(hist_norm = conso_norm, hist_reel = conso) %>%
      pivot_longer(-date, names_to = "colour", values_to = "conso") %>%
      mutate(type = "histo")
    
    pred <- df$pred[[1]] %>%
      pivot_longer(-date, names_to = "colour", values_to = "conso") %>%
      mutate(type = "pred")
    
    data <- bind_rows(histo, pred) %>%
      mutate(conso  = conso / 1000,
             colour = factor(colour, levels = c("hist_reel", "hist_norm",
                                                "fcst_profiler", "fcst_opteam")
             )
      )
    
    ggplot(data = data, mapping = aes(date, conso, colour = colour)) +
      geom_point(size = 1) +
      geom_line(size = 0.5) +
      labs(title = str_c("PCE : ", ref, "   -   CAR GRDF : ", car, " / CAR Profiler : ", new_car)) +
      scale_x_date(date_breaks = "1 month", date_labels = "%b-%y") +
      facet_grid(. ~ type, scales = "free_x", space = "free_x") +
      theme(panel.grid.minor = element_blank(),
            axis.title = element_blank(),
            plot.title = element_text(hjust = 0.5),
            axis.text.x = element_text(angle = 90,
                                       size  = 7.5,
                                       hjust = 0.5,
                                       vjust = 0.5
            ),
            legend.title = element_blank(),
            legend.position = "bottom"
      )
    
  }
  
}

#=============================================#
# Tracer le graphique de consommation agrégée #
#=============================================#

plot_agg <- function(result) {
  
  if (!is_null(result)) {
    
    df <- result %>%
      select(histo) %>%
      unnest(histo) %>%
      group_by(date) %>%
      summarise(hist_reel = sum(conso, na.rm = TRUE), hist_norm = sum(conso_norm), .groups = "drop")
    
    car     <- round(sum(result$car  ) / 1000) %>% format(big.mark = " ")
    new_car <- round(sum(df$hist_norm) / 1000) %>% format(big.mark = " ")
    
    histo <- df %>%
      pivot_longer(-date, names_to = "colour", values_to = "conso") %>%
      mutate(type = "histo")
    
    pred <- result %>%
      select(pred) %>%
      unnest(pred) %>%
      group_by(date) %>%
      summarise(fcst_profiler = sum(fcst_profiler),
                fcst_opteam = sum(fcst_opteam, na.rm = TRUE),
                .groups = "drop"
      ) %>%
      pivot_longer(-date, names_to = "colour", values_to = "conso") %>%
      mutate(type = "pred")
    
    data <- bind_rows(histo, pred) %>%
      mutate(conso = conso / 1000,
             colour = factor(colour, levels = c("hist_reel", "hist_norm",
                                                "fcst_profiler", "fcst_opteam")
             )
      )
    
    ggplot(data = data, mapping = aes(date, conso, colour = colour)) +
      geom_point(size = 1) +
      geom_line(size = 0.5) +
      labs(title = str_c("Aggregate   -   CAR GRDF : ", car, " / CAR Profiler : ", new_car)) +
      scale_x_date(date_breaks = "1 month", date_labels = "%b-%y") +
      facet_grid(. ~ type, scales = "free_x", space = "free_x") +
      theme(panel.grid.minor = element_blank(),
            axis.title = element_blank(),
            plot.title = element_text(hjust = 0.5),
            axis.text.x = element_text(angle = 90,
                                       size  = 7.5,
                                       hjust = 0.5,
                                       vjust = 0.5
            ),
            legend.title = element_blank(),
            legend.position = "bottom"
      )
    
  }
  
}

#===================================================#
# Stocker le carnet de commande dans un fichier RDS #
#===================================================#

write_cc <- function() {
  
  load_cc() %>% write_rds(str_c("run/cc/raw/", today(), ".rds"))
  
}

#========================#
# Fonction de simulation #
#========================#

run_simulation <- function(json_input) {
  
  if (!dir.exists("log")) {dir.create("log")}
  
  logfile <- str_c("log/c3_", format(now(), "%Y-%m-%d_%Hh%Mmin%Ss"), ".log")
  file.create(logfile)
  logger <- create.logger(logfile = logfile, level = "INFO")
  
  info(logger, "Loading client's data & temperatures from JSON")
  input <- fromJSON(json_input)
  
  data_client <- as_tibble(input$donnees_clients) %>%
    rename(simul = id_simulation, start = debut, end = fin, dataconso = donnees_consos) %>%
    mutate(station   = str_pad(station, width=8, pad = "0"),
           start     = ymd(start),
           end       = ymd(end) - days(),
           dataconso = map(dataconso, as_tibble),
           dataconso = map(dataconso, rename, from = debut, to = fin),
           dataconso = map(dataconso, mutate, from = ymd(from), to = ymd(to), conso = conso / 1000),
           pc        = NA_character_,
           zet       = NA_character_,
           operator  = NA_character_,
           tarif     = NA_character_,
           car       = NA_real_,
           freq      = NA_character_,
           opteam    = list(NULL),
           mod       = NA_character_,
           name      = NA_character_
    )
  
  data_temp <- as_tibble(input$temps_stations) %>%
    mutate(station = str_pad(station,  width=8, pad = "0")) %>%
    unnest(temps) %>%
    mutate(date = ymd(date))
  
  rng <- list(from = ymd(input$dates_profiler$calibration_date_debut),
              to   = ymd(input$dates_profiler$calibration_date_fin)
  )
  
  info(logger, "Loading static data from CSV")
  data_static <- load_data_static()
  
  info(logger, "Loading 'bouclage' models from RDS")
  data_model  <- load_data_model()
  
  info(logger, "Checking data consistency")
  if (check_data(data_static, data_client, data_temp, data_model, rng, 0)) {
    
    info(logger, "Forecasting consumptions")
    r <- forecast_JJ(data_client, data_static, data_temp, data_model, rng, 0, FALSE, TRUE)
    
  }
  
  info(logger, "Profiler termination")

  r 
  
}
