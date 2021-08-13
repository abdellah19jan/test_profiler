# Databricks notebook source
   #########      ###         ###
   #########      ###         ###
###         ###   ###         ###
###         ###   ######   ######
###               ######   ######
###               ######   ######
############      ###   ###   ###
############      ###   ###   ###
###         ###   ###   ###   ###
###         ###   ###         ###
###         ###   ###         ###
###         ###   ###         ###
   #########      ###         ###
   #########      ###         ###

#=============================================#
# Construire les sous-historiques mensualisés #
#=============================================#

hist_sub_6M <- function(car,
                        boucl_norm,
                        hist_boucl,
                        conso_ref,
                        ajust_climat,
                        temp_norm,
                        temp_eff,
                        temp_seuil,
                        temp_ref,
                        rng
) {
  
  tibble(date = seq(from = rng$from, to = rng$to - days(), by = "day")) %>%
    mutate(dtmth = floor_date(date, unit = "month"), month = month(date), day = day(date)) %>%
    left_join(boucl_norm  , by = "date") %>%
    left_join(hist_boucl  , by = "date") %>%
    left_join(conso_ref   , by = "date") %>%
    left_join(ajust_climat, by = "dtmth") %>%
    left_join(temp_norm   , by = c("month", "day")) %>%
    left_join(temp_eff    , by = "date") %>%
    left_join(temp_ref    , by = "date") %>%
    mutate(bouclage     = if_else(is.na(bouclage), 1, bouclage),
           ajust_climat = if_else(jour_ouvre, ajust_climat_T, ajust_climat_F),
           var_climat   = pmin(temp, temp_seuil) - pmin(temp_ref, temp_seuil),
           conso_norm   = conso_ref + ajust_climat * var_climat,
           conso_norm   = pmax(conso_norm, 0) * car / 365 * boucl_norm / 1.0026,
           var_climat   = pmin(temp_eff, temp_seuil) - pmin(temp_ref, temp_seuil),
           conso        = conso_ref + ajust_climat * var_climat,
           conso        = pmax(conso, 0) * car / 365 * bouclage / 1.0026
    ) %>%
    group_by(dtmth) %>%
    summarise(conso_norm = sum(conso_norm), conso = sum(conso), .groups = "drop") %>%
    rename(date = dtmth)
  
}

#=============================#
# Mensualiser les historiques #
#=============================#

histo_6M <- function(data_client,
                     data_temp,
                     data_model,
                     bouclage,
                     conso_ref,
                     ajust_climat,
                     temp_seuil,
                     temp_ref,
                     jour_ouvre,
                     rng,
                     with_histo
) {
  
  if (with_histo) {
    
    var_pred <- temp_ref %>%
      filter(between(date, rng$from, rng$to - days())) %>%
      left_join(jour_ouvre, by = "date") %>%
      rename(temp_fr = temp_ref)
    
    boucl_norm <- data_client %>%
      distinct(zet) %>%
      left_join(data_model, by = "zet") %>%
      mutate(data = map(model, add_predictions, data = var_pred, var = "boucl_norm")) %>%
      select(-model) %>%
      unnest(data)
    
    temp_eff <- calc_temp_eff(data_temp)
    
    data_client %>%
      left_join(boucl_norm   %>% nest(boucl_norm   =     -zet), by =     "zet") %>%
      left_join(bouclage     %>% nest(hist_boucl   =     -zet), by =     "zet") %>%
      left_join(conso_ref    %>% nest(conso_ref    =  -profil), by =  "profil") %>%
      left_join(ajust_climat %>% nest(ajust_climat =  -profil), by =  "profil") %>%
      left_join(temp_eff     %>% nest(temp_eff     = -station), by = "station") %>%
      left_join(temp_seuil, by = "station") %>%
      mutate(histo = pmap(list(car, boucl_norm, hist_boucl, conso_ref, ajust_climat,
                               temp_norm, temp_eff, temp_seuil),
                          hist_sub_6M,
                          temp_ref, rng)
      )
    
  } else {
    
    data_client
    
  }
  
}

#===============================================#
#  Construire la matrice du modèle de bouclage  #
# [Y = bouclage, X1 = temp_fr, X2 = jour_ouvre] #
#===============================================#

train_set_6M <- function(bouclage,
                         temp_moy,
                         poid,
                         jour_ouvre
) {
  
  temp_fr <- temp_moy %>%
    left_join(poid, by = "station") %>%
    mutate(temp = poid * temp) %>%
    group_by(date) %>%
    summarise(temp_fr = sum(temp), .groups = "drop")
  
  bouclage %>%
    left_join(temp_fr, by = "date") %>%
    left_join(jour_ouvre, by = "date") %>%
    mutate(jour_ouvre = as_factor(jour_ouvre)) %>%
    select(zet, bouclage, temp_fr, jour_ouvre) %>%
    nest(data = -zet)
  
}

#===============#
# Le modèle GAM #
#===============#

gam_6M <- function(df) {
  
  gam(bouclage ~ s(temp_fr, by = jour_ouvre) + jour_ouvre,
      data   = df,
      method = "REML"
  )
  
}

#====================#
# Calibrer le modèle #
#====================#

fit_6M <- function(train_set) {
  
  train_set %>%
    mutate(model = map(data, possibly(gam_6M, NULL))) %>%
    select(-data)
  
}

#=======================================================================#
# Construire la matrice des variables prédictives du modèle de bouclage #
#=======================================================================#

var_pred_bouclage <- function(data_client,
                              temp_ref,
                              jour_ouvre,
                              dt
) {
  date_pred_start <- min(data_client$start)
  date_pred_end   <- max(data_client$end)
  
  temp_ref %>%
    filter(between(date, date_pred_start, date_pred_end)) %>%
    mutate(temp_ref = temp_ref + dt) %>%
    left_join(jour_ouvre, by = "date") %>%
    rename(temp_fr = temp_ref)
  
}

#===============================================================#
# Appliquer le modèle pour prédire les coefficients de bouclage #
#===============================================================#

pred_bouclage <- function(var_pred,
                          data_client,
                          data_model
) {
  
  data_client %>%
    distinct(zet) %>%
    left_join(data_model, by = "zet") %>%
    mutate(data = map(model, add_predictions, data = var_pred, var = "bouclage")) %>%
    select(-model) %>%
    unnest(data)
  
}

#==================================================================================#
# Construire la matrice des variables prédictives (GRDF + Coefficient de bouclage) #
#==================================================================================#

model_input <- function(bouclage,
                        data_client,
                        temp_norm
) {
  
  data_client %>%
    left_join(bouclage  %>% nest(bouclage  = -zet)    , by = "zet") %>%
    left_join(temp_norm %>% nest(temp_norm = -station), by = "station")
  
}

#========================#
# Fonction de prédiction #
#========================#

pred_6M <- function(start,
                    end,
                    car,
                    opteam,
                    bouclage,
                    conso_ref,
                    ajust_climat,
                    temp_norm,
                    temp_seuil,
                    temp_ref,
                    dt,
                    with_opteam
) {
  
  fcst <- tibble(date = seq(from = start, to = end, by = "day")) %>%
    mutate(dtmth = floor_date(date, unit = "month"), month = month(date), day = day(date)) %>%
    left_join(bouclage    , by = "date") %>%
    left_join(conso_ref   , by = "date") %>%
    left_join(ajust_climat, by = "dtmth") %>%
    left_join(temp_norm   , by = c("month", "day")) %>%
    left_join(temp_ref    , by = "date") %>%
    mutate(ajust_climat = if_else(jour_ouvre, ajust_climat_T, ajust_climat_F),
           var_climat   = pmin(temp + dt, temp_seuil) - pmin(temp_ref, temp_seuil),
           conso        = conso_ref + ajust_climat * var_climat,
           conso        = pmax(conso, 0) * car / 365 * bouclage / 1.0026
    ) %>%
    group_by(dtmth) %>%
    summarise(fcst_profiler = sum(conso), .groups = "drop") %>%
    rename(date = dtmth)
  
  if (with_opteam) {
    
    fcst %>% left_join(opteam, by = c("date" = "dtmth"))
    
  } else {
    
    fcst
    
  }
  
}

#======================================#
# Calculer le FORECAST de consommation #
#======================================#

model_output_6M <- function(data_client,
                            conso_ref,
                            ajust_climat,
                            temp_seuil,
                            temp_ref,
                            dt,
                            with_opteam,
                            with_histo
) {
  
  if (!with_histo) {
    
    data_client <- data_client %>%
      left_join(conso_ref    %>% nest(conso_ref    = -profil), by = "profil") %>%
      left_join(ajust_climat %>% nest(ajust_climat = -profil), by = "profil") %>%
      left_join(temp_seuil, by = "station")
    
  }
  
  if (!with_opteam) {
    
    data_client <- data_client %>% mutate(opteam = vector(mode = "list", length = nrow(data_client)))
    
  }
  
  df <- data_client %>%
    mutate(pred = pmap(list(start, end, car, opteam, bouclage, conso_ref,
                            ajust_climat, temp_norm, temp_seuil),
                       pred_6M,
                       temp_ref, dt, with_opteam)
    )
  
  if (with_histo) {
    
    df %>% select(simul:station, name, profil:car, freq, mod, histo, pred)
    
  } else {
    
    df %>% select(simul:station, name, profil:car, freq, mod, pred)
    
  }
  
}

###         ###   ###         ###
###         ###   ###         ###
###         ###   ###         ###
######   ######   ######   ######
######   ######   ######   ######
######   ######   ######   ######
###   ###   ###   ###   ###   ###
###   ###   ###   ###   ###   ###
###   ###   ###   ###   ###   ###
###         ###   ###         ###
###         ###   ###         ###
###         ###   ###         ###
###         ###   ###         ###
###         ###   ###         ###

#=============================================#
# Construire les sous-historiques mensualisés #
#=============================================#

hist_sub_MM <- function(dataconso,
                        car,
                        bouclage,
                        conso_ref,
                        ajust_climat,
                        temp_eff,
                        temp_seuil,
                        temp_ref,
                        jour_ouvre,
                        rng
) {
  
  dataconso %>%
    mutate(date = map2(from, to - days(), seq, by = "day")) %>%
    unnest(date) %>%
    mutate(dtmth = floor_date(date, unit = "month")) %>%
    left_join(bouclage    , by = "date") %>%
    left_join(conso_ref   , by = "date") %>%
    left_join(ajust_climat, by = "dtmth") %>%
    left_join(temp_eff    , by = "date") %>%
    left_join(temp_ref    , by = "date") %>%
    left_join(jour_ouvre  , by = "date") %>%
    mutate(bouclage     = if_else(is.na(bouclage), 1, bouclage),
           ajust_climat = if_else(jour_ouvre, ajust_climat_T, ajust_climat_F),
           var_climat   = pmin(temp_eff, temp_seuil) - pmin(temp_ref, temp_seuil),
           weight       = conso_ref + ajust_climat * var_climat,
           weight       = pmax(weight, 0) * car * bouclage
    ) %>%
    group_by(from) %>%
    mutate(conso = weight * conso / sum(weight)) %>%
    ungroup() %>%
    filter(between(date, rng$from, rng$to - days())) %>%
    group_by(dtmth) %>%
    summarise(conso = sum(conso), .groups = "drop")
  
}

#=============================#
# Mensualiser les historiques #
#=============================#

histo_MM <- function(data_client,
                     data_temp,
                     bouclage,
                     conso_ref,
                     ajust_climat,
                     temp_seuil,
                     temp_ref,
                     jour_ouvre,
                     rng,
                     with_histo
) {
  
  if (with_histo) {
    
    temp_eff <- calc_temp_eff(data_temp)
    
    data_client %>%
      left_join(bouclage     %>% nest(bouclage     =     -zet), by =     "zet") %>%
      left_join(conso_ref    %>% nest(conso_ref    =  -profil), by =  "profil") %>%
      left_join(ajust_climat %>% nest(ajust_climat =  -profil), by =  "profil") %>%
      left_join(temp_eff     %>% nest(temp_eff     = -station), by = "station") %>%
      left_join(temp_seuil, by = "station") %>%
      mutate(histo = pmap(list(dataconso, car, bouclage, conso_ref, ajust_climat, temp_eff, temp_seuil),
                          hist_sub_MM,
                          temp_ref, jour_ouvre, rng)
      )
    
  } else {
    
    data_client
    
  }
  
}

#================================================#
# Calculer les HDD sur les températures moyennes #
#================================================#

hdd_moy <- function(from,
                    to,
                    temp_moy,
                    temp_seuil
) {
  
  tibble(date = seq(from = from, to = to - days(), by = "day")) %>%
    left_join(temp_moy, by = "date") %>%
    mutate(hdd = pmax(0, temp_seuil - temp)) %>%
    summarise(hdd = sum(hdd)) %>%
    pull()
  
}

#================================================#
# Calculer les HDD sur les températures normales #
#================================================#

hdd_norm <- function(from,
                     to,
                     temp_norm,
                     temp_seuil
) {
  
  tibble(date = seq(from = from, to = to - days(), by = "day")) %>%
    mutate(month = month(date), day = day(date)) %>%
    left_join(temp_norm, by = c("month", "day")) %>%
    mutate(hdd = pmax(0, temp_seuil - temp)) %>%
    summarise(hdd = sum(hdd)) %>%
    pull()
  
}

#==========================#
# Calculer les HDD agrégés #
#==========================#

hdd_agg <- function(dataconso,
                    temp,
                    temp_seuil,
                    hdd_fun
) {
  
  dataconso %>%
    mutate(hdd = map2_dbl(from, to, hdd_fun, temp, temp_seuil))
  
}

#=================================#
# Construire la matrice du modèle #
#      [Y = CONSO, X = HDD]       #
#=================================#

train_set_MM <- function(data_client,
                         data_temp,
                         temp_seuil,
                         with_histo
) {
  
  if (!with_histo) {
    
    data_client <- data_client %>% left_join(temp_seuil, by = "station")
    
  }
  
  data_client %>%
    left_join(data_temp %>% nest(temp_moy = -station), by = "station") %>%
    mutate(dataconso = pmap(list(dataconso, temp_moy, temp_seuil), hdd_agg, hdd_moy))
  
}

#===============#
# Le modèle GAM #
#===============#

gam_MM <- function(profil,
                   df
) {
  
  if (!profil %in% c("P013", "P014")) {
    
    model <- possibly(~ gam(conso ~ s(hdd), data = ., method = "REML"), NULL)(df)
    
    if (is_null(model)) {
      
      lm(conso ~ hdd, data = df)
      
    } else {
      
      model
      
    }
    
  }
  
}

#====================#
# Calibrer le modèle #
#====================#

fit_MM <- function(data_client) {
  
  data_client %>%
    mutate(model     = map2(profil, dataconso, possibly(gam_MM, NULL)),
           dataconso = map2(dataconso,
                            model,
                            ~ possibly(add_residuals, .x %>% mutate(resid = 0))(.x, .y)
           )
    )
  
}

#=========================================================#
# Calculer les consommations journalières à climat normal #
#=========================================================#

conso_norm_day_MM <- function(dataconso,
                              model,
                              conso_ref,
                              rng
) {
  
  dataconso %>%
    possibly(add_predictions, dataconso %>% mutate(pred = conso))(model) %>%
    mutate(pred = pred + resid,
           pred = if_else(hdd == 0 | pred < 0, conso, pred)
    ) %>%
    select(from, to, pred) %>%
    mutate(date = map2(from, to - days(), seq, by = "day")) %>%
    unnest(date) %>%
    left_join(conso_ref, by = "date") %>%
    group_by(from) %>%
    mutate(conso = conso_ref * pred / sum(conso_ref)) %>%
    ungroup() %>%
    filter(between(date, rng$from, rng$to - days())) %>%
    transmute(date, month = month(date), day = day(date), conso)
  
}

#=======================================================#
# Calculer les consommations mensuelles à climat normal #
#=======================================================#

conso_norm_MM <- function(histo,
                          conso_norm
) {
  
  conso_norm %>%
    mutate(dtmth = floor_date(date, unit = "month")) %>%
    group_by(dtmth) %>%
    summarise(conso_norm = sum(conso), .groups = "drop") %>%
    left_join(histo, by = "dtmth") %>%
    rename(date = dtmth)
  
}

#========================#
# Fonction de prédiction #
#========================#

pred_MM <- function(start,
                    end,
                    opteam,
                    conso_norm,
                    with_opteam
) {
  
  fcst <- tibble(date = seq(from = start, to = end, by = "day")) %>%
    mutate(month = month(date), day = day(date), dtmth = floor_date(date, unit = "month")) %>%
    left_join(conso_norm, by = c("month", "day")) %>%
    group_by(dtmth) %>%
    summarise(fcst_profiler = sum(conso, na.rm = TRUE), .groups = "drop") %>%
    rename(date = dtmth)
  
  if (with_opteam) {
    
    fcst %>% left_join(opteam, by = c("date" = "dtmth"))
    
  } else {
    
    fcst
    
  }
  
}

#====================================================#
# Appliquer le modèle pour prédire les consommations #
#====================================================#

model_output_MM <- function(data_client,
                            conso_ref,
                            temp_norm,
                            rng,
                            dt,
                            with_opteam,
                            with_histo
) {
  
  if (!with_histo) {
    
    data_client <- data_client %>%
      left_join(conso_ref %>% nest(conso_ref = -profil), by = "profil")
    
  }
  
  df <- data_client %>%
    left_join(temp_norm %>% nest(temp_norm = -station), by = "station") %>%
    mutate(dataconso  = pmap(list(dataconso, temp_norm, temp_seuil), hdd_agg, hdd_norm),
           conso_norm = pmap(list(dataconso, model, conso_ref), conso_norm_day_MM, rng)
    )
  
  if (with_histo) {
    
    df <- df %>% mutate(histo = map2(histo, conso_norm, conso_norm_MM))
    
  }
  
  if (!with_opteam) {
    
    df <- df %>% mutate(opteam = vector(mode = "list", length = nrow(data_client)))
    
  }
  
  df <- df %>%
    mutate(temp_norm  = map(temp_norm, ~ mutate(., temp = temp + dt)),
           dataconso  = pmap(list(dataconso, temp_norm, temp_seuil), hdd_agg, hdd_norm),
           conso_norm = pmap(list(dataconso, model, conso_ref), conso_norm_day_MM, rng),
           pred       = pmap(list(start, end, opteam, conso_norm), pred_MM, with_opteam)
    )
  
  if (with_histo) {
    
    df %>% select(simul:station, name, profil:car, freq, mod, histo, pred)
    
  } else {
    
    df %>% select(simul:station, name, profil:car, freq, mod, pred)
    
  }
  
}

         ######            ######
         ######            ######
            ###               ###
            ###               ###
            ###               ###
            ###               ###
            ###               ###
            ###               ###
            ###               ###
            ###               ###
###         ###   ###         ###
###         ###   ###         ###
   #########         #########
   #########         #########

#=============================================#
# Construire les sous-historiques mensualisés #
#=============================================#

hist_sub_JJ <- function(dataconso) {
  
  dataconso %>%
    mutate(dtmth = floor_date(from, unit = "month")) %>%
    group_by(dtmth) %>%
    summarise(conso = sum(conso), .groups = "drop")
  
}

#=============================#
# Mensualiser les historiques #
#=============================#

histo_JJ <- function(data_client, with_histo) {
  
  if (with_histo) {
    
    data_client %>% mutate(histo = map(dataconso, hist_sub_JJ))
    
  } else {
    
    data_client
    
  }
  
}

#========================================#
# Construire les sous-matrices du modèle #
#========================================#

train_sub <- function(dataconso,
                      temp_moy,
                      jour_ouvre
) {
  
  dataconso %>%
    select(date = from, conso) %>%
    left_join(temp_moy, by = "date") %>%
    left_join(jour_ouvre, by = "date") %>%
    mutate(month = month(date), day = day(date))
  
}

#=========================================#
#     Construire la matrice du modèle     #
# [Y = CONSO, X1 = temp, X2 = jour_ouvre] #
#=========================================#

train_set_JJ <- function(data_client,
                         data_temp,
                         jour_ouvre
) {
  
  data_client %>%
    left_join(data_temp %>% nest(temp_moy = -station), by = "station") %>%
    mutate(dataconso = map2(dataconso, temp_moy, train_sub, jour_ouvre))
  
}

#===============#
# Le modèle GAM #
#===============#

gam_JJ <- function(profil,
                   df
) {
  
  if (!profil %in% c("P013", "P014")) {
    
    gam(conso ~ s(temp, by = jour_ouvre) + jour_ouvre,
        data   = df %>% mutate(jour_ouvre = as_factor(jour_ouvre)),
        method = "REML"
    )
    
  }
  
}

#========================#
# Calibrer le modèle GAM #
#========================#

fit_gam_JJ <- function(data_client) {
  
  data_client %>% mutate(model = map2(profil, dataconso, possibly(gam_JJ, NULL)))
  
}

#==============================================================#
# Calculer les consommations à climat normal par le modèle GAM #
#==============================================================#

conso_norm_gam_JJ <- function(profil,
                              histo,
                              model,
                              temp_norm,
                              cor_clim,
                              jour_ouvre,
                              rng
) {
  
  if (profil %in% c("P013", "P014") || is_null(model)) {
    
    histo %>%
      left_join(cor_clim, by = "dtmth") %>%
      mutate(conso_norm = conso * cor_clim) %>%
      select(-cor_clim)
    
  } else {
    
    tibble(date = seq(from = rng$from, to = rng$to - days(), by = "day")) %>%
      mutate(month = month(date), day = day(date)) %>%
      left_join(temp_norm, by = c("month", "day")) %>%
      left_join(jour_ouvre, by = "date") %>%
      mutate(dtmth = floor_date(date, unit = "month"), jour_ouvre = as_factor(jour_ouvre)) %>%
      add_predictions(model, var = "conso") %>%
      group_by(dtmth) %>%
      summarise(conso_norm = sum(conso), .groups = "drop") %>%
      left_join(histo, by = "dtmth") %>%
      rename(date = dtmth)
    
  }
  
}

#===========================================================#
# Construire la matrice des variables prédictives du modèle #
#===========================================================#

var_pred_JJ <- function(start,
                        end,
                        temp_norm,
                        jour_ouvre,
                        dt
) {
  
  tibble(date = seq(from = start, to = end, by = "day")) %>%
    mutate(dtmth = floor_date(date, unit = "month"), month = month(date), day = day(date)) %>%
    left_join(temp_norm, by = c("month", "day")) %>%
    mutate(temp = temp + dt) %>%
    left_join(jour_ouvre, by = "date")
  
}

#======================================#
# Fonction de prédiction du modèle GAM #
#======================================#

pred_gam_JJ <- function(profil,
                        dataconso,
                        opteam,
                        model,
                        cor_clim,
                        data,
                        with_opteam
) {
  
  if (profil %in% c("P013", "P014") || is_null(model)) {
    
    df <- dataconso %>%
      mutate(dtmth = floor_date(date, unit = "month")) %>%
      left_join(cor_clim, by = "dtmth") %>%
      mutate(conso_cor = conso * cor_clim) %>%
      select(month, day, conso = conso_cor) %>%
      right_join(data, by = c("month", "day"))
    
  } else {
    
    df <- data %>%
      mutate(jour_ouvre = as_factor(jour_ouvre)) %>%
      add_predictions(model, var = "conso") %>%
      mutate(conso = pmax(0, conso))
    
  }
  
  fcst <- df %>%
    group_by(dtmth) %>%
    summarise(fcst_profiler = sum(conso, na.rm = TRUE), .groups = "drop") %>%
    rename(date = dtmth)
  
  if (with_opteam) {
    
    fcst %>% left_join(opteam, by = c("date" = "dtmth"))
    
  } else {
    
    fcst
    
  }
  
}

#====================================================#
# Appliquer le modèle pour prédire les consommations #
#====================================================#

model_output_JJ <- function(data_client,
                            temp_norm,
                            cor_clim,
                            jour_ouvre,
                            rng,
                            dt,
                            with_opteam,
                            with_histo
) {
  
  if (!with_opteam) {
    
    data_client <- data_client %>% mutate(opteam = vector(mode = "list", length = nrow(data_client)))
    
  }
  
  df <- data_client %>%
    left_join(temp_norm %>% nest(temp_norm = -station), by = "station") %>%
    left_join(cor_clim  %>% nest(cor_clim  = -profil ), by = "profil" ) %>%
    mutate(data = pmap(list(start, end, temp_norm), var_pred_JJ, jour_ouvre, dt),
           pred = pmap(list(profil, dataconso, opteam, model, cor_clim, data), pred_gam_JJ, with_opteam)
    )
  
  if (with_histo) {
    
    df %>%
      mutate(histo = pmap(list(profil, histo, model, temp_norm, cor_clim),
                          conso_norm_gam_JJ,
                          jour_ouvre, rng)
      ) %>%
      select(simul:station, name, profil:car, freq, mod, histo, pred)
    
  } else {
    
    df %>%
      select(simul:station, name, profil:car, freq, mod, pred)
    
  }
  
}

##    ## ##### ##      ##    ###      ##     ####
##    ## ##### ##      ##   #####     ##     #####
##    ## ##### ##      ##   ## ##     ##     ######
####  ## ##    ##      ##   ##      ##  ##   ##  ##
####  ## ##    ##      ##   ##      ##  ##   ##  ##
####  ## ####  ##      ##   ##      ##  ##   ##  ##
##  #### ####   ## ## ##    ##     ##    ##  #####
##  #### ##     ## ## ##    ##     ########  ####
##  #### ##     ## ## ##    ##     ########  ## ##
##    ## #####   ##  ##     ## ## ##      ## ## ##
##    ## #####   ##  ##     ##### ##      ## ##  ##
##    ## #####   ##  ##      ###  ##      ## ##  ##

#========================#
# Fonction de prédiction #
#========================#

pred_new_car <- function(car,
                         start,
                         end,
                         coef_new_car
) {
  
  tibble(date = seq(from = start, to = end, by = "day")) %>%
    mutate(month = month(date), nb = days_in_month(date)) %>%
    left_join(coef_new_car, by = "month") %>%
    mutate(conso = car * coef / nb, date = floor_date(date, unit = "month")) %>%
    group_by(date) %>%
    summarise(fcst_profiler = sum(conso), .groups = "drop")
  
}

#====================================================#
# Appliquer le modèle pour prédire les consommations #
#====================================================#

model_output_new_car <- function(data_client,
                                 coef_new_car
) {
  
  data_client %>%
    left_join(coef_new_car %>% nest(coef_new_car = -c(profil, station)), by = c("profil", "station")) %>%
    mutate(pred = pmap(list(car, start, end, coef_new_car), pred_new_car)) %>%
    select(simul:station, name, profil:car, freq, mod, pred)
  
}
