# Databricks notebook source
#=============================#
# Calculer les CAR par profil #
#=============================#

load_car <- function() {
  
  if (!dir.exists("run/cc/output")) {dir.create("run/cc/output")}
  
  date_min <- readline(prompt = "Date Min (yyyy-mm-dd) : ") %>% ymd()
  date_max <- readline(prompt = "Date Max (yyyy-mm-dd) : ") %>% ymd()
  path_in  <- str_c("run/cc/raw/", get_cc_date(), ".rds")
  path_out <- get_cc_path()
  
  avisia <- load_avisia() %>%
    mutate(car = car / 1000, code_station = str_pad(code_station, 8, pad = "0"))
  
  fix <- read_csv(path_fix, col_types = cols()) %>%
    transmute(date  = mois_volume,
              ratio = (volume_pf + volume_fixe) / (volume_pf + volume_indexe_engagement)
    )
  
  read_rds(path_in) %>%
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
    mutate(profil = if_else(is.na(profil_type), profil_prm, profil_type),
           profil = if_else(profil %in% c("PLAT", "P000"), "P014", profil),
           car    = if_else(is.na(car) | car <= 0, volume_annuel_mwh, car),
           start  = pmax(date_debut, date_min),
           end    = pmin(date_fin, date_max)
    ) %>%
    select(profil, car, start, end) %>%
    mutate(date = map2(start, end, seq, by = "day")) %>%
    select(-start, -end) %>%
    unnest(date) %>%
    group_by(profil, date) %>%
    summarise(car = sum(car), .groups = "drop") %>%
    mutate(date = floor_date(date, unit = "month")) %>%
    group_by(profil, date) %>%
    summarise(car = mean(car), .groups = "drop") %>%
    left_join(fix, by = "date") %>%
    mutate(car = car * ratio) %>%
    select(-ratio) %>%
    pivot_wider(names_from = profil, values_from = car) %>%
    write_csv(path_out)
  
}
