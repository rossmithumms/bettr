readRenviron("/workspaces/brain/.Renviron")
Sys.setenv(SQL_DIR = Sys.getenv("BETTR_SQL_DIR"))

test_that("we can create a table and delete it", {
    result <- bettr::ensure_table(
      connection_name = "app_dqhi_dev",
      table_name = "bettr_test_dummy"
    )

    expect_equal(TRUE, result)

    message("----- we can delete it")
    result <- bettr::drop_table(
      connection_name = "app_dqhi_dev",
      table_name = "bettr_test_dummy"
    )

    expect_equal(TRUE, result)
  }
)

test_that("we can create a table, append data to it, pull data from it, and drop the table", {
    message("----- we can create a table")
    result <- bettr::ensure_table(
      connection_name = "app_dqhi_dev",
      table_name = "bettr_test_data"
    )

    expect_equal(TRUE, result)

    message("----- we can append data to it")
    result <- bettr::append_rows(
      rows = tibble::tibble(
        value_str = c("Anne", "Betsy", "Cathy", "Donna"),
        value_num = c(1, 2, 3, 4),
        value_dt = c(
          lubridate::ymd_hms("20230501 00:00:00"),
          lubridate::ymd_hms("20230502 01:01:01"),
          lubridate::ymd_hms("20230503 02:02:02"),
          lubridate::ymd_hms("20230503 03:03:03")
        )
      ),
      connection_name = "app_dqhi_dev",
      table_name = "bettr_test_data"
    )

    expect_equal(TRUE, result)

    message("----- we can pull data from it")
    actual <- tibble::tibble(
      value_str = c("Anne", "Betsy"),
      value_num = c(1, 2)
    )

    expected <- tibble::tibble(value_num = 2) %>%
      bettr::get_rows(
        connection_name = "app_dqhi_dev",
        sql = "get_bettr_test_data_by_number"
      ) %>%
      tibble::tibble()

    testthat::expect_equal(
      actual %>% dplyr::pull(),
      expected %>% dplyr::pull()
    )

    # message("----- we can delete it")
    # result <- bettr::drop_table(
    #   connection_name = "app_dqhi_dev",
    #   table_name = "bettr_test_data"
    # )

    # expect_equal(TRUE, result)
  }
)
