readRenviron("/workspaces/brain/.Renviron")
Sys.setenv(SQL_DIR = Sys.getenv("BETTR_SQL_DIR"))

test_that("we can create a table and delete it", {
    dummy_data <- tibble::tibble(
      foo = c(1, 2, 3),
      bar = c("a", "b", "c")
    )

    tictoc::tic()
    result <- bettr::ensure_table(
      rows = dummy_data,
      connection_name = "app_dqhi_dev",
      table_name = "bettr_test_dummy"
    )
    tictoc::toc()

    expect_equal(TRUE, result)

    message("----- we can delete it")
    tictoc::tic()
    bettr::drop_table(
      connection_name = "app_dqhi_dev",
      table_name = "bettr_test_dummy"
    )
    tictoc::toc()
  }
)

test_that("we can create a table, append data to it, pull data from it, and drop the table", {
    rows <- tibble::tibble(
      value_str = c("Anne", "Betsy", "Cathy", "Donna"),
      value_num = c(1, 2, 3, 4),
      value_dt = c(
        lubridate::ymd_hms("20230501 00:00:00"),
        lubridate::ymd_hms("20230502 01:01:01"),
        lubridate::ymd_hms("20230503 02:02:02"),
        lubridate::ymd_hms("20230503 03:03:03")
      )
    )

    message("----- we can create a table")
    tictoc::tic()
    result <- bettr::ensure_table(
      rows = rows,
      connection_name = "app_dqhi_dev",
      table_name = "bettr_test_data"
    )
    tictoc::toc()

    expect_equal(TRUE, result)

    message("----- we can append data to it")
    tictoc::tic()
    result <- bettr::append_rows(
      rows = rows,
      connection_name = "app_dqhi_dev",
      table_name = "bettr_test_data",
      suppress_bind_logging = TRUE
    )
    tictoc::toc()

    message("----- we can pull data from it")
    actual <- tibble::tibble(
      value_str = c("Anne", "Betsy"),
      value_num = c(1, 2),
      value_dt = c(
        lubridate::ymd_hms("20230501 00:00:00"),
        lubridate::ymd_hms("20230502 01:01:01")
      )
    )

    tictoc::tic()
    expected <- tibble::tibble(value_num = 2) %>%
      bettr::get_rows(
        connection_name = "app_dqhi_dev",
        sql = "get_bettr_test_data_by_number"
      ) %>%
      tibble::tibble()
    tictoc::toc()

    testthat::expect_equal(
      actual %>% dplyr::select(value_num) %>% dplyr::pull(),
      expected %>% dplyr::select(value_num) %>% dplyr::pull()
    )

    testthat::expect_equal(
      actual %>% dplyr::select(value_str) %>% dplyr::pull(),
      expected %>% dplyr::select(value_str) %>% dplyr::pull()
    )

    testthat::expect_equal(
      actual %>% dplyr::select(value_dt) %>% dplyr::pull() %>% format(format = "%H:%M:%S"),
      expected %>% dplyr::select(value_dt) %>% dplyr::pull() %>% format(format = "%H:%M:%S")
    )

    message("----- we can delete it")
    tictoc::tic()
    bettr::drop_table(
      connection_name = "app_dqhi_dev",
      table_name = "bettr_test_data"
    )
    tictoc::toc()
  }
)
