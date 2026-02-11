library(dplyr)
library(lubridate)
library(arrow)
library(readr)
library(climindi)
library(zendown)
library(cli)
library(tibble)

cli_alert_info("Loading files...")
prec_1950_2022 <- zen_file(10036212, "total_precipitation_sum.parquet")
prec_2023 <- zen_file(10947952, "total_precipitation_sum.parquet")
prec_2024 <- zen_file(15748125, "total_precipitation_sum.parquet")
prec_2025 <- zen_file(18257037, "total_precipitation_sum.parquet")

cli_alert_info("Processing files...")
prec_data <- open_dataset(
  sources = c(prec_1950_2022, prec_2023, prec_2024, prec_2025)
) |>
  # Average precipitation
  filter(name == "total_precipitation_sum_mean") |>
  # Time filter
  filter(date >= as.Date("1981-01-01")) |>
  # Unit conversion from m to mm
  mutate(value = round(value * 1000, digits = 2)) |>
  select(-name) |>
  arrange(code_muni, date) |>
  collect()

cli_alert_info("Computing normal...")
prec_normal <- prec_data |>
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

ufs <- prec_data |>
  mutate(uf = substr(code_muni, 0, 2)) |>
  select(uf) |>
  distinct(uf) |>
  pull(uf)

prec_indi <- tibble()

for (i in ufs) {
  cli_inform("{i}")

  prec_indi_tmp <- prec_data |>
    # Identify year
    mutate(year = year(date)) |>
    # Filter year
    filter(year >= 2011) |>
    # Filter UF
    filter(substr(code_muni, 0, 2) == i) |>
    # Identify week
    mutate(week = epiweek(date)) |>
    # Create wave variables
    dplyr::group_by(code_muni) |>
    dplyr::arrange(date) |>
    add_wave(
      normals_df = prec_normal,
      threshold = 0,
      threshold_cond = "gte",
      size = 3,
      var_name = "rs3"
    ) |>
    add_wave(
      normals_df = prec_normal,
      threshold = 0,
      threshold_cond = "gte",
      size = 5,
      var_name = "rs5"
    ) |>
    dplyr::ungroup() |>
    # Group by id variable, year and week
    group_by(code_muni, year, week) |>
    # Compute precipitation indicators
    summarise_precipitation(
      value_var = value,
      normals_df = prec_normal
    ) |>
    # Ungroup
    ungroup()

  prec_indi <- bind_rows(prec_indi, prec_indi_tmp)
  rm(prec_indi_tmp)
}


cli_alert_info("Exporting...")
write_parquet(x = prec_normal, sink = "prec_normal_weekly_1981_2010.parquet")
write_csv2(x = prec_normal, file = "prec_normal_weekly_1981_2010.csv")
write_parquet(x = prec_indi, sink = "prec_indi_weekly_1981_2010.parquet")
write_csv2(x = prec_indi, file = "prec_indi_weekly_1981_2010.csv")
