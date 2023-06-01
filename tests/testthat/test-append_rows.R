readRenviron("/workspaces/brain/.Renviron")
Sys.setenv(SQL_DIR = Sys.getenv("BETTR_SQL_DIR"))
if(isNamespaceLoaded("tidyverse") == FALSE) {
  attachNamespace("tidyverse")
}

test_that("write a data set to a new table", {
  rows <- tibble::tibble(
    foo_str = c("boo", "bar", "baz"),
    foo_nbr = c(1, 2, 3),
    foo_date = c(
      lubridate::ymd_hms("20230501 00:00:00"),
      lubridate::ymd_hms("20230501 23:59:59"),
      lubridate::ymd("20230501")
    )
  )

  table_name <- paste0("test_append_rows_", lubridate::now() %>% format("%Y%m%d%H%M%S"))
  message(stringr::str_glue("+++ table_name for test: {table_name}"))

  result <- append_rows(
    rows = rows,
    connection_name = "app_dqhi_dev",
    table_name = table_name
  )

  message(result)
})

test_that("write a data set to an existing table", {

})
