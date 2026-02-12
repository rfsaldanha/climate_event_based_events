# Packages
library(dplyr)
library(lubridate)
library(arrow)
library(zendown)
library(readr)
library(zip)

# Setup
start_year <- 2010
end_year <- 2025

# Data sources
prec_n1961_1990 <- zen_file(18612340, "prec_weekly_indi_n1961_1990.parquet")
prec_n1981_2010 <- zen_file(18612340, "prec_weekly_indi_n1981_2010.parquet")
tmax_n1961_1990 <- zen_file(18612340, "tmax_weekly_indi_n1961_1990.parquet")
tmax_n1981_2010 <- zen_file(18612340, "tmax_weekly_indi_n1981_2010.parquet")
tmin_n1961_1990 <- zen_file(18612340, "tmin_weekly_indi_n1961_1990.parquet")
tmin_n1981_2010 <- zen_file(18612340, "tmin_weekly_indi_n1981_2010.parquet")
rh_n1961_1990 <- zen_file(18612340, "rh_indi_weekly_n1961_1990.parquet")
rh_n1981_2010 <- zen_file(18612340, "rh_indi_weekly_n1981_2010.parquet")
ws_n1961_1990 <- zen_file(18612340, "ws_weekly_indi_n1961_1990.parquet")
ws_n1981_2010 <- zen_file(18612340, "ws_weekly_indi_n1981_2010.parquet")

# Precipitation
prec_a <- open_dataset(sources = prec_n1961_1990) |>
  filter(year >= start_year & year <= 2010) |>
  collect()

prec_b <- open_dataset(sources = prec_n1981_2010) |>
  filter(year >= 2011 & year <= end_year) |>
  collect()

prec <- bind_rows(prec_a, prec_b) |>
  mutate(
    across(
      .cols = c(year, week, count, rs3:d_25),
      .fns = as.integer
    ),
    across(
      .cols = normal_mean:p90,
      .fns = \(x) round(x, 2)
    )
  ) |>
  select(code_muni, year, week, mean) |>
  mutate(code_muni = as.integer(substr(code_muni, 0, 6)))

rm(prec_a, prec_b)

write_delim(x = prec, file = "prec_ocs.csv", delim = ";")
zip(zipfile = "prec_ocs.csv.zip", files = "prec_ocs.csv")
unlink(x = "prec_ocs.csv")

rm(prec)

# Tmax
tmax_a <- open_dataset(sources = tmax_n1961_1990) |>
  filter(year >= start_year & year <= 2010) |>
  collect()

tmax_b <- open_dataset(sources = tmax_n1981_2010) |>
  filter(year >= 2011 & year <= end_year) |>
  collect()

tmax <- bind_rows(tmax_a, tmax_b) |>
  mutate(
    across(
      .cols = c(year, week, count, hot_days:t_40),
      .fns = as.integer
    ),
    across(
      .cols = normal_mean:p90,
      .fns = \(x) round(x, 2)
    )
  ) |>
  select(code_muni, year, week, mean, hw3) |>
  mutate(code_muni = as.integer(substr(code_muni, 0, 6)))

rm(tmax_a, tmax_b)

write_delim(x = tmax, file = "tmax_ocs.csv", delim = ";")
zip(zipfile = "tmax_ocs.csv.zip", files = "tmax_ocs.csv")
unlink(x = "tmax_ocs.csv")

rm(tmax)

# Tmin
tmin_a <- open_dataset(sources = tmin_n1961_1990) |>
  filter(year >= start_year & year <= 2010) |>
  collect()

tmin_b <- open_dataset(sources = tmin_n1981_2010) |>
  filter(year >= 2011 & year <= end_year) |>
  collect()

tmin <- bind_rows(tmin_a, tmin_b) |>
  mutate(
    across(
      .cols = c(year, week, count, cw3:t_20),
      .fns = as.integer
    ),
    across(
      .cols = normal_mean:p90,
      .fns = \(x) round(x, 2)
    )
  ) |>
  select(code_muni, year, week, mean) |>
  mutate(code_muni = as.integer(substr(code_muni, 0, 6)))

rm(tmin_a, tmin_b)

write_delim(x = tmin, file = "tmin_ocs.csv", delim = ";")
zip(zipfile = "tmin_ocs.csv.zip", files = "tmin_ocs.csv")
unlink(x = "tmin_ocs.csv")

rm(tmin)

# ws
ws_a <- open_dataset(sources = ws_n1961_1990) |>
  filter(year >= start_year & year <= 2010) |>
  collect()

ws_b <- open_dataset(sources = ws_n1981_2010) |>
  filter(year >= 2011 & year <= end_year) |>
  collect()

ws <- bind_rows(ws_a, ws_b) |>
  mutate(
    across(
      .cols = c(year, week, count, l_u2_3:b12),
      .fns = as.integer
    ),
    across(
      .cols = normal_mean:p90,
      .fns = \(x) round(x, 2)
    )
  ) |>
  select(code_muni, year, week, mean) |>
  mutate(code_muni = as.integer(substr(code_muni, 0, 6))) |>
  mutate(mean = round(mean * 3.6, 2))

rm(ws_a, ws_b)

write_delim(x = ws, file = "ws_ocs.csv", delim = ";")
zip(zipfile = "ws_ocs.csv.zip", files = "ws_ocs.csv")
unlink(x = "ws_ocs.csv")

rm(ws)

# rh
rh_a <- open_dataset(sources = rh_n1961_1990) |>
  filter(year >= start_year & year <= 2010) |>
  collect()

rh_b <- open_dataset(sources = rh_n1981_2010) |>
  filter(year >= 2011 & year <= end_year) |>
  collect()

rh <- bind_rows(rh_a, rh_b) |>
  mutate(
    across(
      .cols = c(year, week, count, ds3:h_11),
      .fns = as.integer
    ),
    across(
      .cols = normal_mean:p90,
      .fns = \(x) round(x, 2)
    )
  ) |>
  select(code_muni, year, week, mean) |>
  mutate(code_muni = as.integer(substr(code_muni, 0, 6)))

rm(rh_a, rh_b)

write_delim(x = rh, file = "rh_ocs.csv", delim = ";")
zip(zipfile = "rh_ocs.csv.zip", files = "rh_ocs.csv")
unlink(x = "rh_ocs.csv")

rm(rh)
