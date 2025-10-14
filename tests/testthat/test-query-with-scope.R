readRenviron("/workspaces/brain/.Renviron")
readRenviron("/workspaces/brain/.Renviron.test")
Sys.setenv(SQL_DIR = Sys.getenv("BETTR_SQL_DIR"))
Sys.setenv(BETTR_AT_SCOPE = "bettr_test")
test_tz <- Sys.getenv("TZ")

withr::defer(
  {
    tryCatch(
      {
        bettr::drop_table(
          connection_name = "app_dqhi_dev",
          table_name = "bettr_test_data_@@@@"
        )
      },
      error = \(e) {}
    )
  }
)

testthat::test_that(
  "we can create a table with scope, append/pull data, and drop the table",
  {
    rows <- tibble::tibble(
      value_str = c("Anne", "Betsy", "Cathy", "Donna"),
      value_num = c(1, 2, 3, 4),
      value_dt = c(
        lubridate::ymd_hms("20230501 00:00:00", tz = test_tz),
        lubridate::ymd_hms("20230502 01:01:01", tz = test_tz),
        lubridate::ymd_hms("20230503 02:02:02", tz = test_tz),
        lubridate::ymd_hms("20230503 03:03:03", tz = test_tz)
      )
    )

    message("----- basics: we can create a table with scope and append data to it")
    result <- bettr::append_rows(
      rows = rows,
      connection_name = "app_dqhi_dev",
      table_name = "bettr_test_data_@@@@",
      suppress_bind_logging = TRUE
    )

    message("----- we can pull data from it")
    actual <- tibble::tibble(
      value_str = c("Anne", "Betsy"),
      value_num = c(1, 2),
      value_dt = c(
        lubridate::ymd_hms("20230501 00:00:00"),
        lubridate::ymd_hms("20230502 01:01:01")
      )
    )

    expected <- bettr::get_rows(
      connection_name = "app_dqhi_dev",
      sql = "get_bettr_test_data_with_scope"
    ) |>
      tibble::tibble()

    testthat::expect_equal(
      rows |> dplyr::summarise(n = dplyr::n()),
      expected |> dplyr::summarise(n = dplyr::n())
    )

    expected <- tibble::tibble(value_num = 2) |>
      bettr::get_rows(
        connection_name = "app_dqhi_dev",
        sql = "get_bettr_test_data_by_number_with_scope"
      ) |>
      tibble::tibble()

    testthat::expect_equal(
      actual |> dplyr::select(value_num) |> dplyr::pull(),
      expected |> dplyr::select(value_num) |> dplyr::pull()
    )

    testthat::expect_equal(
      actual |> dplyr::select(value_str) |> dplyr::pull(),
      expected |> dplyr::select(value_str) |> dplyr::pull()
    )

    testthat::expect_equal(
      actual |> dplyr::select(value_dt) |> 
        dplyr::pull() |> format(format = "%H:%M:%S"),
      expected |> dplyr::select(value_dt) |>
        dplyr::pull() |> format(format = "%H:%M:%S")
    )

    message("----- we can delete it")
    bettr::drop_table(
      connection_name = "app_dqhi_dev",
      table_name = "bettr_test_data_@@@@"
    )
  }
)

testthat::test_that(
  "We can run one or more scoped transactions with zero, one, or many bind rows",
  {
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

    message("----- transactions: we can create a table with scope and append data to it")
    result <- bettr::append_rows(
      rows = rows,
      connection_name = "app_dqhi_dev",
      table_name = "bettr_test_data_@@@@",
      suppress_bind_logging = TRUE
    )

    message("----- we can append redundant data without suppressed logs")
    result <- bettr::append_rows(
      rows = rows,
      connection_name = "app_dqhi_dev",
      table_name = "bettr_test_data_@@@@"
    )

    # test that there are 8 rows
    testthat::expect_equal(
      8,
      tibble::tibble(value_num = 4) |>
        bettr::get_rows(
          connection_name = "app_dqhi_dev",
          sql = "get_bettr_test_data_by_number_with_scope"
        ) |>
        dplyr::count() |>
        dplyr::pull()
    )

    message("---- we can run a single scoped transaction, no bind rows")
    bettr::execute_stmts(
      connection_name = "app_dqhi_dev",
      sql_file = "one_tx_no_bind_rows_with_scope"
    )

    # test that no records with value_num = 1 remain
    testthat::expect_equal(
      0,
      tibble::tibble(value_num = 1) |>
        bettr::get_rows(
          connection_name = "app_dqhi_dev",
          sql = "get_bettr_test_data_by_number_with_scope"
        ) |>
        dplyr::count() |>
        dplyr::pull()
    )

    message("---- we can run multiple scoped transactions, no bind rows")
    bettr::execute_stmts(
      connection_name = "app_dqhi_dev",
      sql_file = "many_tx_no_bind_rows_with_scope"
    )

    # test that no records with value_num in (2, 4) remain
    testthat::expect_equal(
      0,
      tibble::tibble(value_num = 4) |>
        bettr::get_rows(
          connection_name = "app_dqhi_dev",
          sql = "get_bettr_test_data_by_number_with_scope"
        ) |>
        dplyr::filter(value_num %in% c(2, 4)) |>
        dplyr::count() |>
        dplyr::pull()
    )

    message("---- we can run a single scoped transaction, one bind row")
    tibble::tibble(
      value_str = "Erica",
      value_num = 5
    ) |>
      bettr::execute_stmts(
        connection_name = "app_dqhi_dev",
        sql_file = "one_tx_one_bind_row_with_scope"
      )

    # test that there is one new row with value_num = 5, named Erica
    testthat::expect_equal(
      1,
      tibble::tibble(value_num = 5) |>
        bettr::get_rows(
          connection_name = "app_dqhi_dev",
          sql = "get_bettr_test_data_by_number_with_scope"
        ) |>
        dplyr::filter(value_num == 5, value_str == "Erica") |>
        dplyr::count() |>
        dplyr::pull()
    )

    message("---- we can run multiple scoped transactions, one bind row")
    tibble::tibble(
      value_str = "Francine",
      value_num = 6
    ) |>
      bettr::execute_stmts(
        connection_name = "app_dqhi_dev",
        sql_file = "many_tx_one_bind_row_with_scope",
      )

    # test that there is no longer a row with value_num = 5
    testthat::expect_equal(
      0,
      tibble::tibble(value_num = 5) |>
        bettr::get_rows(
          connection_name = "app_dqhi_dev",
          sql = "get_bettr_test_data_by_number_with_scope"
        ) |>
        dplyr::filter(value_num == 5) |>
        dplyr::count() |>
        dplyr::pull()
    )

    # test that there is a row with value_num = 6, named Francine
    testthat::expect_equal(
      1,
      tibble::tibble(value_num = 6) |>
        bettr::get_rows(
          connection_name = "app_dqhi_dev",
          sql = "get_bettr_test_data_by_number_with_scope"
        ) |>
        dplyr::filter(value_num == 6, value_str == "Francine") |>
        dplyr::count() |>
        dplyr::pull()
    )

    message("---- we can run multiple scoped transactions, many bind rows")
    tibble::tibble(
        add_value_str = c("Georgia", "Henrietta"),
        add_value_num = c(7, 8),
        delete_value_num = c(5, 6)
      ) |>
      bettr::execute_stmts(
        connection_name = "app_dqhi_dev",
        sql_file = "many_tx_many_bind_rows_with_scope",
      )

    # test that there are no longer any rows with value_num in (5, 6)
    testthat::expect_equal(
      0,
      tibble::tibble(value_num = 8) |>
        bettr::get_rows(
          connection_name = "app_dqhi_dev",
          sql = "get_bettr_test_data_by_number_with_scope"
        ) |>
        dplyr::filter(value_num %in% c(5, 6)) |>
        dplyr::count() |>
        dplyr::pull()
    )

    # test that there are two new rows with value_num (7, 8)
    testthat::expect_equal(
      2,
      tibble::tibble(value_num = 8) |>
        bettr::get_rows(
          connection_name = "app_dqhi_dev",
          sql = "get_bettr_test_data_by_number_with_scope"
        ) |>
        dplyr::filter(value_num %in% c(7, 8)) |>
        dplyr::count() |>
        dplyr::pull()
    )
  }
)
