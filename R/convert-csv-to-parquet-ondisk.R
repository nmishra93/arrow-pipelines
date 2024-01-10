library(here)
library(fs)
library(arrow)
library(dbplyr)
library(tictoc)

abstar_outputs <-
  fs::dir_ls(
    here::here(
      "PROCESSED_RUNS",
      "tabular"
    )
  )

# tracking processing time
tic()

open_csv_dataset(abstar_outputs,
  unify_schemas = TRUE,
  convert_options = CsvConvertOptions$create(strings_can_be_null = TRUE)
) |>
  mutate(
    # Add file ID to later extract animal ID
    animal = add_filename(),
    # `str_extract` not supproted in `arrow` or `duckdb` # nolint
    # so wrote custom SQL to get animal ID, that didn't work either.
    # animal = "REGEXP_EXTRACT(animal, '[BC]\\w\\d+')", # nolint
    timepoint = "WK4"
  ) |>
  group_by(animal, d_gene) |>
  write_dataset(
    here::here(
      "data",
      "processed",
      "parquet_files"
    ),
    format = "parquet"
  )

toc()
