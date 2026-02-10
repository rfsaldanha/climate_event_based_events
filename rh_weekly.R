library(dplyr)
library(lubridate)
library(arrow)
library(readr)
library(climindi) # http://rfsaldanha.github.io/climindi/
library(zendown) # https://rfsaldanha.github.io/zendown/
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
rh_indi_tmp <- tibble()
for (i in 2011:2025) {
  cli_inform("{i}")

  rh_indi_tmp <- rh_data |>
    # Identify year
    mutate(year = year(date)) |>
    # Identify week
    mutate(month = epiweek(date)) |>
    # Filter year
    filter(year == i) |>
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

  ws_indi <- bind_rows(ws_indi, ws_indi_tmp)
  rm(ws_indi_tmp)
}


cli_alert_info("Exporting...")
write_parquet(x = rh_normal, sink = "rh_normal.parquet")
write_csv2(x = rh_normal, file = "rh_normal.csv")
write_parquet(x = rh_indi, sink = "rh_indi.parquet")
write_csv2(x = rh_indi, file = "rh_indi.csv")
