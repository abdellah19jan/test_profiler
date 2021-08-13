# Databricks notebook source
#===============================================================================#
# Extraire les coefficients de bouclage correspondant à la période de calibrage #
#===============================================================================#

bouclage_subset <- function(bouclage_all) {

  date_subset <- bouclage_all %>%
    distinct(date) %>%
    arrange(date) %>%
    mutate(month = month(date), day = day(date)) %>%
    group_by(month, day) %>%
    summarise(date = max(date), .groups = "drop") %>%
    pull()
  
  bouclage_all %>% filter(date %in% date_subset)
  
}

#==============================================================================#
# Choisir le modèle de prédiction (JJ/MM/6M) en fonction de la data disponible #
#==============================================================================#

freq_rlv <- function(dataconso,
                     rng
) {
  
  if (is_null(dataconso) || nrow(dataconso) == 0) {
    
    "6M"
    
  } else {
    
    start              <- dataconso$from
    start[[1]]         <- max(start[[1]], rng$from)
    end                <- dataconso$to
    end[[length(end)]] <- min(end[[length(end)]], rng$to)
    
    if (all(start[-1] >= end[-length(end)]) && sum((start %--% end) %/% days()) >= 360) {
      
      if (setequal(dataconso$from + days(), dataconso$to)) {
        
        "JJ"
        
      } else {
        
        if (nrow(dataconso) >= 12) {
          
          "MM"
          
        } else {
          
          "6M"
          
        }
        
      }
      
    } else {
      
      "6M"
      
    }
    
  }
  
}

#==================================#
# Mettre en forme les températures #
#==================================#

shape <- function(l) {
  
  transpose(l) %>%
    as_tibble() %>%
    unnest(c(Value, Date)) %>%
    transmute(date = ymd(str_sub(Date, end = 10L)), temp = Value)
  
}

#======================================================#
# Calculer les températures efficaces pour une station #
#======================================================#

aux <- function(data) {
  
  data %>%
    arrange(date) %>%
    mutate(temp_eff = 0.64 * temp + 0.24 * lag(temp) + 0.12 * lag(temp, n = 2L)) %>%
    filter(!is.na(temp_eff))
  
}

#=====================================#
# Calculer les températures efficaces #
#=====================================#

calc_temp_eff <- function(data_temp) {
  
  data_temp %>%
    nest(data = -station) %>%
    mutate(data = map(data, aux)) %>%
    unnest(data) %>%
    select(-temp)
  
}

#=================================================#
# Vérifier s'il y a des incohérences dans la data #
#=================================================#

check_data <- function(data_static,
                       data_client,
                       data_temp,
                       data_model,
                       rng,
                       dt,
                       with_opteam
) {
  
  dic <- read_csv(path_dic_pce, col_types = cols())
  
  vect_day     <- read_csv(path_vect_day    , col_types = cols())
  vect_profil  <- read_csv(path_vect_profil , col_types = cols())
  vect_station <- read_csv(path_vect_station, col_types = cols())
  
  bool <- TRUE
  
  #-----------------------------#
  # Vérifier la table CONSO_REF #
  #-----------------------------#
  
  colnames_ref <- c("profil", "date", "conso_ref")
  
  if (bool && !setequal(colnames_ref, colnames(data_static$conso_ref))) {
    
    dlg_message("conso_ref tibble has a wrong format")
    bool <- FALSE
    
  }
  
  if (bool && !is.Date(data_static$conso_ref$date)) {
    
    dlg_message("date column of conso_ref tibble is not a date vector")
    bool <- FALSE
    
  }
  
  if (bool && is.na(data_static$conso_ref$date) %>% any()) {
    
    dlg_message("date column of conso_ref tibble has some NAs")
    bool <- FALSE
    
  }
  
  date_start <- data_client %>%
    filter(!map_lgl(dataconso, ~ is_null(.) || nrow(.) == 0)) %>%
    mutate(min_from = map_dbl(dataconso, ~ summarise(., min(from)) %>% pull()) %>%
             as_date()
    ) %>%
    summarise(min(min_from, rng$from)) %>%
    pull()
  date_end   <- max(data_client$end)
  date_min   <- min(data_static$conso_ref$date)
  date_max   <- max(data_static$conso_ref$date)
  
  if (bool && (date_start < date_min || date_max < date_end)) {
    
    dlg_message("Some dates are missing in conso_ref tibble")
    bool <- FALSE
    
  }
  
  date_rng     <- seq(from = date_min, to = date_max, by = "day")
  profil_d_ref <- expand_grid(vect_profil, date = date_rng)
  profil_d     <- select(data_static$conso_ref, profil, date)
  
  if (bool && !setequal(profil_d_ref, profil_d)) {
    
    dlg_message("Entries in conso_ref tibble (profil, date) are not consistent")
    bool <- FALSE
    
  }
  
  if (bool && !is_double(data_static$conso_ref$conso_ref)) {
    
    dlg_message("conso_ref column of conso_ref tibble is not numeric")
    bool <- FALSE
    
  }
  
  if (bool && is.na(data_static$conso_ref$conso_ref) %>% any()) {
    
    dlg_message("conso_ref column of conso_ref tibble has some NAs")
    bool <- FALSE
    
  }
  
  if (bool && any(data_static$conso_ref$conso_ref < 0)) {
    
    dlg_message("Some values in conso_ref column of conso_ref tibble are negative")
    bool <- FALSE
    
  }
  
  #----------------------------#
  # Vérifier la table TEMP_REF #
  #----------------------------#
  
  colnames_ref <- c("date", "temp_ref")
  
  if (bool && !setequal(colnames_ref, colnames(data_static$temp_ref))) {
    
    dlg_message("temp_ref tibble has a wrong format")
    bool <- FALSE
    
  }
  
  if (bool && !is.Date(data_static$temp_ref$date)) {
    
    dlg_message("date column of temp_ref tibble is not a date vector")
    bool <- FALSE
    
  }
  
  if (bool && is.na(data_static$temp_ref$date) %>% any()) {
    
    dlg_message("date column of temp_ref tibble has some NAs")
    bool <- FALSE
    
  }
  
  date_min <- min(data_static$temp_ref$date)
  date_max <- max(data_static$temp_ref$date)
  date_rng <- seq(from = date_min, to = date_max, by = "day")
  
  if (bool && (date_start < date_min || date_max < date_end   ||
               !setequal(data_static$temp_ref$date, date_rng))
  ) {
    
    dlg_message("Some dates are missing in temp_ref tibble")
    bool <- FALSE
    
  }
  
  if (bool && !is_double(data_static$temp_ref$temp_ref)) {
    
    dlg_message("temp_ref column of temp_ref tibble is not numeric")
    bool <- FALSE
    
  }
  
  if (bool && is.na(data_static$temp_ref$temp_ref) %>% any()) {
    
    dlg_message("temp_ref column of temp_ref tibble has some NAs")
    bool <- FALSE
    
  }
  
  #------------------------------#
  # Vérifier la table JOUR_OUVRE #
  #------------------------------#
  
  colnames_ref <- c("date", "jour_ouvre")
  
  if (bool && !setequal(colnames_ref, colnames(data_static$jour_ouvre))) {
    
    dlg_message("jour_ouvre tibble has a wrong format")
    bool <- FALSE
    
  }
  
  if (bool && !is.Date(data_static$jour_ouvre$date)) {
    
    dlg_message("date column of jour_ouvre tibble is not a date vector")
    bool <- FALSE
    
  }
  
  if (bool && is.na(data_static$jour_ouvre$date) %>% any()) {
    
    dlg_message("date column of jour_ouvre tibble has some NAs")
    bool <- FALSE
    
  }
  
  date_min   <- min(data_static$jour_ouvre$date)
  date_max   <- max(data_static$jour_ouvre$date)
  date_rng   <- seq(from = date_min, to = date_max, by = "day")
  
  if (bool && (date_start < date_min || date_max < date_end   ||
               !setequal(data_static$jour_ouvre$date, date_rng))
  ) {
    
    dlg_message("Some dates are missing in jour_ouvre tibble")
    bool <- FALSE
    
  }
  
  if (bool && !is_logical(data_static$jour_ouvre$jour_ouvre)) {
    
    dlg_message("jour_ouvre column of jour_ouvre tibble is not logical")
    bool <- FALSE
    
  }
  
  if (bool && is.na(data_static$jour_ouvre$jour_ouvre) %>% any()) {
    
    dlg_message("jour_ouvre column of jour_ouvre tibble has some NAs")
    bool <- FALSE
    
  }
  
  #--------------------------------#
  # Vérifier la table AJUST_CLIMAT #
  #--------------------------------#
  
  colnames_ref <- c("profil", "dtmth", "ajust_climat_F", "ajust_climat_T")
  
  if (bool && !setequal(colnames_ref, colnames(data_static$ajust_climat))) {
    
    dlg_message("ajust_climat tibble has a wrong format")
    bool <- FALSE
    
  }
  
  if (bool && !is.Date(data_static$ajust_climat$dtmth)) {
    
    dlg_message("dtmth column of ajust_climat tibble is not a date vector")
    bool <- FALSE
    
  }
  
  if (bool && is.na(data_static$ajust_climat$dtmth) %>% any()) {
    
    dlg_message("dtmth column of ajust_climat tibble has some NAs")
    bool <- FALSE
    
  }
  
  date_end <- floor_date(date_end, unit = "month")
  date_min <- min(data_static$ajust_climat$dtmth)
  date_max <- max(data_static$ajust_climat$dtmth)
  
  if (bool && (date_start < date_min || date_max < date_end)) {
    
    dlg_message("Some dates are missing in ajust_climat tibble")
    bool <- FALSE
    
  }
  
  date_rng     <- seq(from = date_min, to = date_max, by = "month")
  profil_d_ref <- expand_grid(vect_profil, dtmth = date_rng)
  profil_d     <- select(data_static$ajust_climat, profil, dtmth)
  
  if (bool && !setequal(profil_d_ref, profil_d)) {
    
    dlg_message("Entries in ajust_climat tibble (profil, dtmth) are not consistent")
    bool <- FALSE
    
  }
  
  if (bool && (!is_double(data_static$ajust_climat$ajust_climat_F) ||
      !is_double(data_static$ajust_climat$ajust_climat_T))
  ) {
    
    dlg_message("ajust_climat_... column of ajust_climat tibble is not numeric")
    bool <- FALSE
    
  }
  
  if (bool && (is.na(data_static$ajust_climat$ajust_climat_F) %>% any() ||
      is.na(data_static$ajust_climat$ajust_climat_T) %>% any())
  ) {
    
    dlg_message("ajust_climat_... column of ajust_climat tibble has some NAs")
    bool <- FALSE
    
  }
  
  #------------------------------#
  # Vérifier la table TEMP_SEUIL #
  #------------------------------#
  
  colnames_ref <- c("station", "temp_seuil")
  
  if (bool && !setequal(colnames_ref, colnames(data_static$temp_seuil))) {
    
    dlg_message("temp_seuil tibble has a wrong format")
    bool <- FALSE
    
  }
  
  col_station <- select(data_static$temp_seuil, station)
  
  if (bool && !setequal(vect_station, col_station)) {
    
    dlg_message("Entries in temp_seuil tibble (station) are not consistent")
    bool <- FALSE
    
  }
  
  if (bool && !is_double(data_static$temp_seuil$temp_seuil)) {
    
    dlg_message("temp_seuil column of temp_seuil tibble is not numeric")
    bool <- FALSE
    
  }
  
  if (bool && is.na(data_static$temp_seuil$temp_seuil) %>% any()) {
    
    dlg_message("temp_seuil column of temp_seuil tibble has some NAs")
    bool <- FALSE
    
  }
  
  #-----------------------------#
  # Vérifier la table TEMP_NORM #
  #-----------------------------#
  
  colnames_ref <- c("station", "month", "day", "temp")
  
  if (bool && !setequal(colnames_ref, colnames(data_static$temp_norm))) {
    
    dlg_message("temp_norm tibble has a wrong format")
    bool <- FALSE
    
  }
  
  station_d_ref <- expand_grid(vect_station, vect_day)
  station_d     <- select(data_static$temp_norm, station, month, day)
  
  if (bool && !setequal(station_d_ref, station_d)) {
    
    dlg_message("Entries in temp_norm tibble (station, month, day) are not consistent")
    bool <- FALSE
    
  }
  
  if (bool && !is_double(data_static$temp_norm$temp)) {
    
    dlg_message("temp column of temp_norm tibble is not numeric")
    bool <- FALSE
    
  }
  
  if (bool && is.na(data_static$temp_norm$temp) %>% any()) {
    
    dlg_message("temp column of temp_norm tibble has some NAs")
    bool <- FALSE
    
  }
  
  #-------------------------------#
  # Vérifier la table DATA_CLIENT #
  #-------------------------------#
  
  colnames_ref <- c("simul", "pc", "pce", "station", "profil", "zet", "operator", "tarif",
                    "car", "start", "end", "freq", "dataconso", "mod", "name"
  )
  
  if (with_opteam) {
    
    colnames_ref <- c(colnames_ref, "opteam")
    
  }
  
  if (bool && !setequal(colnames_ref, colnames(data_client))) {
    
    dlg_message("data_client tibble has a wrong format")
    bool <- FALSE
    
  }
  
  if (bool && is.na(data_client$simul) %>% any()) {
    
    dlg_message("simul column of data_client tibble has some NAs")
    bool <- FALSE
    
  }
  
  if (bool && is.na(data_client$pce) %>% any()) {
    
    dlg_message("pce column of data_client tibble has some NAs")
    bool <- FALSE
    
  }
  
  # if (bool && nrow(data_client) != n_distinct(data_client$pce)) {
  #   
  #   dlg_message("Some PCE appear more than once in data_client tibble")
  #   bool <- FALSE
  #   
  # }
  
  if (bool && !all(data_client$station %in% pull(vect_station))) {
    
    dlg_message("Some stations in data_client tibble are wrong")
    bool <- FALSE
    
  }
  
  if (bool && !all(data_client$profil %in% pull(vect_profil))) {
    
    dlg_message("Some profiles in data_client tibble are wrong")
    bool <- FALSE
    
  }
  
  if (bool && !all(data_client$zet %in% c(ZET, NA))) {
    
    dlg_message("Some ZETs in data_client tibble are wrong")
    bool <- FALSE
    
  }
  
  if (bool && !is_double(data_client$car)) {
    
    dlg_message("car column of data_client tibble is not numeric")
    bool <- FALSE
    
  }
  
  # if (bool && is.na(data_client$car) %>% any()) {
  #   
  #   dlg_message("car column of data_client tibble has some NAs")
  #   bool <- FALSE
  #   
  # }
  
  # if (bool && any(data_client$car <= 0)) {
  #   
  #   dlg_message("Some values in car column of data_client tibble are not positive")
  #   bool <- FALSE
  #   
  # }
  
  if (bool && (!is.Date(data_client$start) || !is.Date(data_client$end))) {
    
    dlg_message("start/end column of data_client tibble is not a date vector")
    bool <- FALSE
    
  }
  
  if (bool && (is.na(data_client$start) %>% any() || is.na(data_client$end) %>% any())) {
    
    dlg_message("start/end column of data_client tibble has some NAs")
    bool <- FALSE
    
  }
  
  if (bool && any(data_client$start < today())) {
    
    dlg_message("Some start dates are in the past")
    bool <- FALSE
    
  }
  
  if (bool && any(data_client$end < data_client$start)) {
    
    dlg_message("Some start dates are posterior to corresponding end dates")
    bool <- FALSE
    
  }
  
  nb <- data_client %>%
    distinct(pce) %>%
    left_join(dic, by = "pce") %>%
    filter(!is.na(li)) %>%
    count(li) %>%
    filter(n > 1) %>%
    nrow()
  
  if (bool && nb > 0) {
    
    dlg_message("More than one PCE correspond to the same LI")
    bool <- FALSE
    
  }
  
  #-----------------------------#
  # Vérifier la table DATA_TEMP #
  #-----------------------------#
  
  if (bool && is_null(data_temp)) {
    
    dlg_message("Temperature data are unavailable in Mercure")
    bool <- FALSE
    
  }
  
  #------------------------------#
  # Vérifier la table DATA_MODEL #
  #------------------------------#
  
  if (bool && map_lgl(data_model$model, is_null) %>% any()) {
    
    dlg_message("Some models are null")
    bool <- FALSE
    
  }
  
  #----------------------------------#
  # Vérifier le delta de température #
  #----------------------------------#
  
  if (bool && is.na(dt)) {
    
    dlg_message("dt is not a double")
    bool <- FALSE
    
  }
  
  bool
  
}
