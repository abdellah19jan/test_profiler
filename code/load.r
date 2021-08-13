# Databricks notebook source
#==============================================================#
# Charger les consommations normées à température de référence #
#==============================================================#

load_conso_ref <- function() {
  
  read_csv(path_conso_ref, col_types = cols())
  
}

#==========================================================#
# Charger la table qui indique si un jour est ouvré ou pas #
#==========================================================#

load_jour_ouvre <- function() {
  
  read_csv(path_jour_ouvre, col_types = cols())
  
}

#==================================================#
# Charger les coefficients d'ajustement climatique #
#==================================================#

load_ajust_climat <- function() {
  
  read_csv(path_ajust_climat, col_types = cols())
  
}

#===================================#
# Charger les températures normales #
#===================================#

load_temp_norm <- function() {
  
  read_csv(path_temp_norm, col_types = cols(station = col_character()))
  
}

#=======================================#
# Charger les températures de référence #
#=======================================#

load_temp_ref <- function() {
  
  read_csv(path_temp_ref, col_types = cols())
  
}

#================================#
# Charger les températures seuil #
#================================#

load_temp_seuil <- function() {
  
  read_csv(path_temp_seuil, col_types = cols())
  
}

#===============================================#
# Charger la table des coefficients de bouclage #
#===============================================#

load_bouclage <- function() {
  
  con <- dbConnect(Postgres(),
                   dbname   = "c3_gaz", 
                   host     = "dtcoprdpgs004l.pld.infrasys16.com", 
                   port     = 5435, 
                   user     = "c3gaz_reader",
                   password = "97TBqjAZz64KkLFYq4PKhphfi2V8RFtRQVgunX9xw63H"
  )
  
  query <- str_c("select * ",
                 "from estimate_data.bouclage_zet ",
                 "where date_bouclage >= '", DATE_MIN_BOUCLAGE, "'",
                 "and zet in ('ZET04', 'ZET06')"
  )
  
  df <- dbGetQuery(con, query) %>% as_tibble()
  
  dbDisconnect(con)
  
  df %>%
    group_by(zet, date_bouclage) %>%
    arrange(desc(integration_date)) %>%
    filter(row_number() == 1) %>%
    ungroup() %>%
    select(zet, date = date_bouclage, bouclage = k2_zet)
  
}

#================================#
# Charger les poids des stations #
#================================#

load_poid <- function() {
  
  read_csv(path_poid, col_types = cols())
  
}

#=====================#
# Charger les modèles #
#=====================#

load_data_model <- function() {

  dir <- list.dirs(path = path_model, recursive = FALSE) %>% max()
  
  tibble(zet = ZET, model = str_c(dir, "/", zet, ".rds") %>% map(possibly(read_rds, NULL)))
  
}

#==============================#
# Charger les données de OMEGA #
#==============================#

load_omega <- function(pce, rng) {
  
  r <- POST(url    = "https://gem.okta-emea.com/oauth2/aus2jp8vmz3yYrDnc0i7/v1/token",
            body   = list(grant_type    = "client_credentials",
                          client_id     = "3-s-downstream-forecasting-connector-7KPFVGH0H6GYMU7",
                          client_secret = "7e03uKQWLdCnsQqqIfdrpg16kYa8sOLIMtPSDk9O",
                          scope         = "api.wattson.supplydata.omega.read"
            ),
            encode = "form"
  )
  
  access_token <- content(r)$access_token
  
  payload <- list(
    points = list(
      list(
        autorisationConso                  = "F",
        autorisationConsoEtendue           = TRUE,
        autorisationSituationContractuelle = TRUE,
        consultationConsoPCELibre          = FALSE,
        mandat                             = "true",
        numeroPce                          = pce,
        plageConsoDebut                    = rng$from,
        plageConsoFin                      = rng$to + months(1),
        typeOccupant                       = "Actuel"
      )
    )
  )
  
  r <- POST(url    = "https://external-requests.downstream-prd.ncd.infrasys16.com/Omega/consumption",
            config = add_headers("Content-Type" = "application/json",
                                 accept         = "*/*",
                                 Authorization  = str_c("Bearer ", access_token)
            ),
            body   = payload,
            encode = "json"
  )
  
  if (r$status_code == 200) {
    
    consump <- content(r)[[1]]$consumptions
    
    if (length(consump) > 0) {
      
      l <- transpose(consump) %>% map(unlist)
      
      df <- l[!map_lgl(l, is_null)] %>%
        as_tibble() %>%
        transmute(from  = ymd_hms(startDate) %>% as_date(),
                  to    = ymd_hms(endDate) %>% as_date(),
                  to    = if_else(from == to, to + days(), to),
                  conso = as.double(quantity)
        ) %>%
        filter(from < rng$to, to > rng$from, from < to, !is.na(conso))
      
      if (nrow(df) == 0) {
        
        NULL
        
      } else {
        
        df %>% arrange(from)
        
      }
      
    }
    
  } else {
    
    NULL
    
  }
  
}

#===========================================#
# Charger les consommations via API WATTSON #
#===========================================#

load_wattson <- function(pce, operator, rng) {
  
  pce     <- str_remove(pce, "^PIC")
  url     <- str_c(URL_WATTSON, "volume", "gas", operator, pce, sep = "/")
  url_idx <- str_c(url, "consumption-index", sep = "/")
  start   <- format(rng$from, "%Y-%m-%dT00:00:00Z")
  end     <- format(rng$to  , "%Y-%m-%dT00:00:00Z")
  key_val <- glue("start_date={start}",
                  "end_date={end}",
                  "date_format=iso",
                  .sep = "&"
  )
  
  r <- GET(url = str_c(url_idx, key_val, sep = "?"), add_headers(HEADERS))
  
  if (r$status_code == 200) {
    
    l <- content(r, as = "text", encoding = "UTF-8") %>% fromJSON()
    
    if (l$length == 0) {
      
      r <- GET(url = str_c(url, key_val, sep = "?"), add_headers(HEADERS))
      
      if (r$status_code == 200) {
        
        l <- content(r, as = "text", encoding = "UTF-8") %>% fromJSON()
        
        if (l$length == 0) {
          
          NULL
          
        } else {
          
          mat           <- l$items
          colnames(mat) <- l$columns
          
          df <- as_tibble(mat) %>%
            transmute(from  = ymd(str_sub(timestamp, end = 10L)),
                      to    = from + days(),
                      conso = as.double(value)
            ) %>%
            filter(!is.na(conso))
          
          if (nrow(df) == 0) {
            
            NULL
            
          } else {
            
            df %>% arrange(from)
            
          }
          
        }
        
      } else {
        
        NULL
        
      }
      
    } else {
      
      df <- as_tibble(l$consumption_indexes) %>%
        mutate(from  = ymd(str_sub(series_start_date, end = 10L)),
               to    = ymd(str_sub(series_end_date  , end = 10L)),
               conso = as.double(value)
        ) %>%
        arrange(desc(series_integration_date)) %>%
        group_by(from) %>%
        filter(row_number() == 1) %>%
        ungroup() %>%
        group_by(to) %>%
        filter(row_number() == 1) %>%
        ungroup() %>%
        select(from, to, conso) %>%
        arrange(from)
      
      nb        <- nrow(df)
      bool      <- vector(length = nb)
      bool[[1]] <- TRUE
      
      if (nb > 1) {
        
        tp <- df$to[[1]]
        
        for (i in 2:nb) {
          
          if (df$from[[i]] >= tp) {
            
            bool[[i]] <- TRUE
            tp        <- df$to[[i]]
            
          }
          
        }
        
      }
      
      df <- df %>% filter(bool, from < to, !is.na(conso))
      
      if (nrow(df) == 0) {
        
        NULL
        
      } else {
        
        df %>% arrange(from)
        
      }
      
    }
    
  } else {
    
    NULL
    
  }
  
}

#==============================================#
# Charger les consommations de WATTSON & OMEGA #
#==============================================#

load_conso <- function(pce, operator, freq, tarif, rng, logger) {
  
  info(logger, str_c("Loading ", pce, "'s historical consumptions from WattsOn"))
  
  dataconso <- load_wattson(pce, operator, rng)
  
  if (is_null(dataconso)) {
    
    info(logger, str_c("Loading ", pce, "'s historical consumptions from Omega"))
    
    dataconso <- load_omega(pce, rng)
    
  }
  
  if (!is_null(dataconso)) {
    
    dataconso %>% mutate(conso = conso / 1002.6) %>% filter(conso < 15000)
    
  }
  
}

#===========================#
# Charger les consommations #
#===========================#

load_archive_conso <- function(pce, operator, freq, tarif, rng, logger) {
  
  if (freq %in% c("MM", "JJ", NA) &&
      !tarif %in% c("T1", "T2") &&
      !str_detect(pce, "^\\d") &&
      !str_detect(pce, "/")
  ) {
    
    path <- str_c("conso/", pce, "_", rng$from, "_", rng$to, ".csv")
    
    if (file.exists(path)) {
      
      dataconso <- read_csv(path, col_types = cols())
      
    } else {
      
      dataconso <- load_conso(pce, operator, freq, tarif, rng, logger)
      
      if (is_null(dataconso)) {
        
        write_csv(tibble(from = vector(), to = vector(), conso = vector()), path)
        
      } else {
        
        write_csv(dataconso, path)
        
      }
      
    }
    
  }
  
}

#===========================================#
# Charger les données du carnet de commande #
#===========================================#

load_cc <- function() {
  
  con <- dbConnect(odbc(),
                   driver   = "ODBC Driver 17 for SQL Server",
                   server   = "sqlbackend-prd-sql01.database.windows.net",
                   database = "engie.sideddme-prd.database",
                   uid      = "DDM270@sqlbackend-prd-sql01",
                   pwd      = "Sp*PaF2eTh_ye$Ef"
  )
  
  query <- str_c("select * from dtm_carnet_commande_gaz_opteam_edm ",
                 "where annee_import = (select max(annee_import) from dtm_carnet_commande_gaz_opteam_edm)"
  )
  
  df <- dbGetQuery(con, query) %>% as_tibble()
  
  dbDisconnect(con)
  
  df
  
}

#==============================================#
# Charger les données du suivi des engagements #
#==============================================#

load_se <- function() {
  
  con <- dbConnect(odbc(),
                   driver   = "ODBC Driver 17 for SQL Server",
                   server   = "sqlbackend-prd-sql01.database.windows.net",
                   database = "engie.sideddme-prd.database",
                   uid      = "DDM270@sqlbackend-prd-sql01",
                   pwd      = "Sp*PaF2eTh_ye$Ef"
  )
  
  df <- dbReadTable(con, "dtm_suivi_engagement_gaz_opteam_edm") %>% as_tibble()
  
  dbDisconnect(con)
  
  df
  
}

#===============================#
# Charger les données de AVISIA #
#===============================#

load_avisia <- function() {
  
  con <- dbConnect(Postgres(),
                   dbname   = "c3_gaz", 
                   host     = "dtcoprdpgs004l.pld.infrasys16.com", 
                   port     = 5435, 
                   user     = "c3gaz_reader",
                   password = "97TBqjAZz64KkLFYq4PKhphfi2V8RFtRQVgunX9xw63H"
  )
  
  query <- str_c("select id_pce, car, profil_type, code_station, freq_releve, bu",
                 "from gas_feeder.raw_import_avisia",
                 "where month_volume = (select max(month_volume) from gas_feeder.raw_import_avisia)",
                 sep = " "
  )
  
  df <- dbGetQuery(con, query) %>% as_tibble()
  
  dbDisconnect(con)
  
  df
  
}

#==============================================#
# Charger les données de la vue PERIMETER_VIEW #
#==============================================#

load_perim <- function() {
  
  con <- dbConnect(Postgres(),
                   dbname   = "c3_gaz", 
                   host     = "dtcoprdpgs004l.pld.infrasys16.com", 
                   port     = 5435, 
                   user     = "c3gaz_reader",
                   password = "97TBqjAZz64KkLFYq4PKhphfi2V8RFtRQVgunX9xw63H"
  )
  
  query <- "select * from gas_feeder.perimeter_view"
  
  df <- dbGetQuery(con, query) %>% as_tibble()
  
  dbDisconnect(con)
  
  df
  
}

#===============================================================#
# Charger les données relatives au client du carnet de commande #
#===============================================================#

load_dc_from_cc <- function(path, with_opteam) {
  
  date_min <- ceiling_date(today(), unit = "month")
  date_max <- DATE_MAX_FCST
  
  avisia <- load_avisia() %>%
    mutate(car = car / 1000, code_station = str_pad(code_station, 8, pad = "0"))
  
  if (path == "") {
    
    cc <- load_cc()
    
  } else {
    
    cc <- read_rds(path)
    
  }
  
  dc_from_cc <- cc %>%
    group_by(num_prm) %>%
    arrange(date_debut, date_signature_pc) %>%
    mutate(date_fin = pmin(date_fin, lead(date_debut) - days(), na.rm = TRUE)) %>%
    ungroup() %>%
    filter(date_debut <= date_fin,
           date_debut <= date_max,
           date_fin >= date_min,
           !str_starts(statut_contrat, "^En c|^R"),
           type_offre == "Prix individualisé"
    ) %>%
    anti_join(filter(avisia, bu != "E&C"), by = c("num_prm" = "id_pce")) %>%
    left_join(avisia, by = c("num_prm" = "id_pce")) %>%
    mutate(simul    = if_else(is.na(num_simulation), "0", num_simulation),
           pce      = if_else(is.na(num_prm), "indef", num_prm),
           station  = if_else(is.na(code_station), code_station_meteo, code_station),
           station  = case_when(is.na(station)        ~ "75114001",
                                station == "74024003" ~ "74042003",
                                station == "51183001" ~ "51449002",
                                station == "89346001" ~ "89295001",
                                TRUE                  ~ station
           ),
           profil   = if_else(is.na(profil_type), profil_prm, profil_type),
           profil   = if_else(profil %in% c("PLAT", "P000"), "P014", profil),
           zet      = if_else(zone_equilibrage %in% c(NA_character_, "ZN", "ZS"), "ZET04", "ZET06"),
           operator = case_when(gestionnaire_reseau == "GDFT" ~ "GRT",
                                gestionnaire_reseau == "TIGF" ~ "TIGF",
                                TRUE                          ~ "GRDF"
           ),
           car      = if_else(is.na(car) | car <= 0, volume_annuel_mwh, car),
           start    = pmax(date_debut, date_min),
           end      = pmin(date_fin, date_max)
    )
  
  dc <- dc_from_cc %>%
    select(simul,
           pc    = ref_proposition_commerciale,
           pce,
           station,
           profil,
           zet,
           operator,
           tarif = tarif_d_acheminement_prm,
           car,
           start,
           end,
           freq  = freq_releve
    )
  
  if (with_opteam) {
    
    df <- dc_from_cc %>%
      select(simul, pce, date_debut_vecteur, ma0:ma60) %>%
      pivot_longer(ma0:ma60, names_to = "dtmth", values_to = "fcst_opteam") %>%
      filter(!is.na(fcst_opteam)) %>%
      mutate(dtmth = as_date(date_debut_vecteur) +
               str_remove(dtmth, "ma") %>% as.double() %>% months()) %>%
      select(-date_debut_vecteur) %>%
      nest(opteam = dtmth:fcst_opteam)
    
    dflt <- tibble(dtmth = Date(), fcst_opteam = double())
    
    dc %>%
      left_join(df, by = c("simul", "pce")) %>%
      mutate(opteam = map(opteam, ~ if (is_null(.)) {dflt} else {.}))
    
  } else {
    
    dc
    
  }
  
}

#==================================================================#
# Charger les données relatives au client du suivi des engagements #
#==================================================================#

load_dc_from_se <- function(path) {
  
  dc_from_se <- read_excel(path) %>%
    mutate(pc       = NA_character_,
           code     = suppressWarnings(as.double(`N° PCE`)),
           pce      = if_else(is.na(code), `N° PCE`, str_pad(`N° PCE`, 14, pad = "0")),
           station  = str_pad(`Code station météo`, 8, pad = "0"),
           profil   = if_else(is.na(`Profil GRD retenu`),
                              `Profil GRD (Hmy)`,
                              `Profil GRD retenu`
           ),
           zet      = case_when(`Zone d'équilibrage` %in% c("ZN", "ZS") ~ "ZET04",
                                `Zone d'équilibrage` == "TIGF"          ~ "ZET06",
                                TRUE                                    ~ NA_character_
           ),
           operator = if_else(`Réseau de raccordement` == "Distribution",
                              "GRDF",
                              if_else(zet == "ZET04", "GRT", "TIGF")
           ),
           car      = if_else(is.na(CAR), `Quantité annuelle déclarée`, as.double(CAR)),
           start    = as_date(`Date d'effet site`),
           end      = as_date(`Date de fin du contrat`) - days(),
           freq     = NA_character_
    )
  
  df <- dc_from_se %>%
    select(simul = `N° de simulation`, pce, start, MA0:MA95) %>%
    pivot_longer(MA0:MA95, names_to = "dtmth", values_to = "fcst_opteam") %>%
    filter(!is.na(fcst_opteam)) %>%
    mutate(dtmth = floor_date(start, unit = "month") +
             str_remove(dtmth, "MA") %>% as.double() %>% months()
    ) %>%
    select(-start) %>%
    nest(opteam = dtmth:fcst_opteam)
  
  dc_from_se %>%
    select(simul = `N° de simulation`,
           pc,
           pce,
           station,
           profil,
           zet,
           operator,
           tarif = `Tarif ATRD`,
           car,
           start,
           end,
           freq
    ) %>%
    left_join(df, by = c("simul", "pce"))
  
}

#====================================================================#
# Charger les données relatives au client de la table PERIMETER_VIEW #
#====================================================================#

load_dc_from_perim <- function() {
  
  perim <- load_perim()
  
  dc_from_perim <- perim %>%
    mutate(tarif    = NA_character_,
           operator = case_when(str_starts(pce, "PIC") ~ "TIGF",
                                str_starts(pce, "LI")  ~ "GRT",
                                TRUE                   ~ "GRDF"
           ),
           zet      = if_else(operator == "TIGF", "ZET06", "ZET04"),
           end      = mois_volume + months(1) - days(),
           opteam   = vector(mode = "list", length = nrow(perim))
    ) %>%
    select(simul   = id_simulation_opteam,
           pc      = ref_proposition_commerciale_rubis,
           pce,
           station = station_meteo,
           profil,
           zet,
           operator,
           tarif,
           car,
           start   = mois_volume,
           end,
           freq    = freq_releve,
           opteam
    )
  
}

#=========================================#
# Charger les données relatives au client #
#=========================================#

load_data_client <- function(path_se, path_cc, rng, with_opteam, logger) {
  
  dic_pce     <- read_csv(path_dic_pce    , col_types = cols())
  dic_station <- read_csv(path_dic_station, col_types = cols())
  
  if (path_se == "") {
    
    df <- load_dc_from_cc(path_cc, with_opteam)
    
  } else {
    
    df <- load_dc_from_se(path_se)
    
  }
  
  df %>%
    left_join(dic_pce, by = "pce") %>%
    mutate(li        = if_else(is.na(li), pce, li),
           dataconso = pmap(list(li, operator, freq, tarif), load_archive_conso, rng, logger),
           mod       = map_chr(dataconso, freq_rlv, rng)
    ) %>%
    select(-li) %>%
    left_join(dic_station, by = "station")
  
}

#==================================================#
# Charger les températures moyennes depuis Mercure #
#==================================================#

load_temp_moy <- function(station, start, end) {
  
  r <- POST(url    = "https://gem.okta-emea.com/oauth2/aus2jp8vmz3yYrDnc0i7/v1/token",
            body   = list(grant_type    = "client_credentials",
                          client_id     = "3-s-downstream-forecasting-connector-7KPFVGH0H6GYMU7",
                          client_secret = "7e03uKQWLdCnsQqqIfdrpg16kYa8sOLIMtPSDk9O",
                          scope         = "api.mercure"
            ),
            encode = "form"
  )
  
  access_token <- content(r)$access_token
  
  dic    <- read_csv(path_dic_moy, col_types = cols(id_ref = col_character()))
  id_ref <- tibble(station) %>% left_join(dic, by = "station") %>% pull()
  
  payload <- list(
    IncludeOffset = TRUE,
    ApplicationDate = list(
      From = start + days(),
      To = end + days()
    ),
    Items = to_list(for(i in seq_along(id_ref))
      list(
        IdRef = id_ref[[i]],
        MaturityType = "D",
        Sliding = list(
          From = -1,
          To = -1
        ),
        GroupingMode = 0,
        FormulaVarName = str_c("x", as.character(i))
      )
    )
  )
  
  temp_moy <- POST(url    = "https://api.gem.myengie.com/internal/mercure/Curves",
                   config = add_headers("Content-Type" = "application/json",
                                        Accept         = "application/json",
                                        Authorization  = str_c("Bearer ", access_token)
                   ),
                   body   = payload,
                   encode = "json"
  ) %>%
    content() %>%
    transpose() %>%
    as_tibble() %>%
    transmute(id_ref = IdRef, data = map(Points, shape)) %>%
    unnest(c(id_ref, data)) %>%
    mutate(id_ref = as.character(id_ref), date = date - days())
  
  expand_grid(station, date = seq(from = start, to = end, by = "day")) %>%
    left_join(dic, by = "station") %>%
    left_join(temp_moy, by = c("id_ref", "date")) %>%
    select(-id_ref)
  
}

#============================================#
# Charger les coefficients du modèle NEW CAR #
#============================================#

load_coef_new_car <- function() {
  
  read_csv(path_coef_new_car, col_types = cols())
  
}

#==================================================================#
# Charger le MAPPING entre les fréquences de relève et les modèles #
#==================================================================#

load_map <- function() {
  
  read_csv(path_map, col_types = cols())
  
}

#===================================================#
# Charger les coefficients de correction climatique #
#===================================================#

load_cor_clim <- function() {
  
  pattern <- "ratio_consommation_corrigee_consommation_brute_profil_|_grdf"
  
  GET(URL_OPENDATA) %>%
    content() %>%
    pluck("records") %>%
    transpose() %>%
    as_tibble() %>%
    mutate(fields = map(fields, as_tibble)) %>%
    unnest(datasetid:record_timestamp) %>%
    select(mois_annee, starts_with("ratio_con")) %>%
    pivot_longer(-mois_annee, names_to = "profil", values_to = "cor_clim") %>%
    transmute(profil = str_remove_all(profil, pattern) %>% str_to_upper(),
              dtmth  = ymd(mois_annee),
              cor_clim
    ) %>%
    arrange(profil, dtmth)
  
}
