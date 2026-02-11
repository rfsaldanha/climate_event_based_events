library(dplyr)
library(lubridate)
library(arrow)
library(readr)
library(climindi)
library(zendown)
library(cli)
library(tibble)

cli_alert_info("Loading files...")
rh_1950_2022 <- zen_file(18392587, "rh_mean_mean_2022_1950.parquet")
rh_2023 <- zen_file(18392587, "rh_mean_mean_2023.parquet")
rh_2024 <- zen_file(18392587, "rh_mean_mean_2024.parquet")
rh_2025 <- zen_file(18392587, "rh_mean_mean_2025.parquet")

cli_alert_info("Processing files...")
rh_data <- open_dataset(sources = c(rh_1950_2022, rh_2023, rh_2024, rh_2025)) |>
  # Average relative humidity
  filter(name == "rh_mean_mean") |>
  # Filter period
  filter(date >= as.Date("1981-01-01")) |>
  select(-name) |>
  arrange(code_muni, date) |>
  collect()

cli_alert_info("Computing normal...")
rh_normal <- rh_data |>
  # Identify week
  mutate(week = epiweek(date)) |>
  # Group by id variable and week
  group_by(code_muni, week) |>
  # Compute normal
  summarise_normal(
    date_var = date,
    value_var = value,
    year_start = 1981,
    year_end = 2010
  ) |>
  # Ungroup
  ungroup()

cli_alert_info("Computing indicators...")

ufs <- rh_data |>
  mutate(uf = substr(code_muni, 0, 2)) |>
  select(uf) |>
  distinct(uf) |>
  pull(uf)

rh_indi <- tibble()

for (i in ufs) {
  cli_inform("{i}")

  rh_indi_tmp <- rh_data |>
    # Identify year
    mutate(year = year(date)) |>
    # Filter year
    filter(year >= 2011) |>
    # Filter UF
    filter(substr(code_muni, 0, 2) == i) |>
    # Identify week
    mutate(week = epiweek(date)) |>
    # Create wave variables
    group_by(code_muni) |>
    add_wave(
      normals_df = rh_normal,
      threshold = -10,
      threshold_cond = "lte",
      size = 3,
      var_name = "ds3"
    ) |>
    add_wave(
      normals_df = rh_normal,
      threshold = -10,
      threshold_cond = "lte",
      size = 5,
      var_name = "ds5"
    ) |>
    add_wave(
      normals_df = rh_normal,
      threshold = 10,
      threshold_cond = "lte",
      size = 3,
      var_name = "ws3"
    ) |>
    add_wave(
      normals_df = rh_normal,
      threshold = 10,
      threshold_cond = "lte",
      size = 5,
      var_name = "ws5"
    ) |>
    ungroup() |>
    # Group by id variable, year and week
    group_by(code_muni, year, week) |>
    # Compute precipitation indicators
    summarise_rel_humidity(
      value_var = value,
      normals_df = rh_normal
    ) |>
    # Ungroup
    ungroup()

  rh_indi <- bind_rows(rh_indi, rh_indi_tmp)
  rm(rh_indi_tmp)
}


cli_alert_info("Exporting...")
write_parquet(x = rh_normal, sink = "rh_weekly_normal_n1981_2010.parquet")
write_csv2(x = rh_normal, file = "rh_weekly_normal_n1981_2010.csv")
write_parquet(x = rh_indi, sink = "rh_weekly_indi_n1981_2010.parquet")
write_csv2(x = rh_indi, file = "rh_weekly_indi_n1981_2010.csv")
