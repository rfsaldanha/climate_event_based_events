library(dplyr)
library(lubridate)
library(arrow)
library(readr)
library(climindi)
library(zendown)
library(cli)
library(tibble)

cli_alert_info("Loading files...")
tmax_1950_2022 <- zen_file(10036212, "2m_temperature_max.parquet")
tmax_2023 <- zen_file(10947952, "2m_temperature_max.parquet")
tmax_2024 <- zen_file(15748125, "2m_temperature_max.parquet")
tmax_2025 <- zen_file(18257037, "2m_temperature_max.parquet")

cli_alert_info("Processing files...")
tmax_data <- open_dataset(
  sources = c(tmax_1950_2022, tmax_2023, tmax_2024, tmax_2025)
) |>
  # Average maximum temperature
  filter(name == "2m_temperature_max_mean") |>
  # Time filter
  filter(date >= as.Date("1961-01-01")) |>
  # Unit conversion form Kelvin to Celsius degrees
  mutate(value = round(value - 273.15, digits = 2)) |>
  select(-name) |>
  arrange(code_muni, date) |>
  collect()

cli_alert_info("Computing normal...")
tmax_normal <- tmax_data |>
  # Identify week
  mutate(week = epiweek(date)) |>
  # Group by id variable and week
  group_by(code_muni, week) |>
  # Compute normal
  summarise_normal(
    date_var = date,
    value_var = value,
    year_start = 1961,
    year_end = 1990
  ) |>
  # Ungroup
  ungroup()

cli_alert_info("Computing indicators...")

ufs <- tmax_data |>
  mutate(uf = substr(code_muni, 0, 2)) |>
  select(uf) |>
  distinct(uf) |>
  pull(uf)

tmax_indi <- tibble()
for (i in ufs) {
  cli_inform("{i}")

  tmax_indi_tmp <- tmax_data |>
    # Identify year
    mutate(year = year(date)) |>
    # Filter year
    filter(year >= 1991) |>
    # Filter UF
    filter(substr(code_muni, 0, 2) == i) |>
    # Identify week
    mutate(week = epiweek(date)) |>
    # Create wave variables
    group_by(code_muni) |>
    add_wave(
      normals_df = tmax_normal,
      threshold = 5,
      threshold_cond = "gte",
      size = 3,
      var_name = "hw3"
    ) |>
    add_wave(
      normals_df = tmax_normal,
      threshold = 5,
      threshold_cond = "gte",
      size = 5,
      var_name = "hw5"
    ) |>
    ungroup() |>
    # Group by id variable, year and week
    group_by(code_muni, year, week) |>
    # Compute precipitation indicators
    summarise_temp_max(
      value_var = value,
      normals_df = tmax_normal
    ) |>
    # Ungroup
    ungroup()

  tmax_indi <- bind_rows(tmax_indi, tmax_indi_tmp)
  rm(tmax_indi_tmp)
}


cli_alert_info("Exporting...")
write_parquet(x = tmax_normal, sink = "tmax_weekly_normal_n1961_1990.parquet")
write_csv2(x = tmax_normal, file = "tmax_weekly_normal_n1961_1990.csv")
write_parquet(x = tmax_indi, sink = "tmax_weekly_indi_n1961_1990.parquet")
write_csv2(x = tmax_indi, file = "tmax_weekly_indi_n1961_1990.csv")
