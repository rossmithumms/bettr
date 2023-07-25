readRenviron("/workspaces/brain/.Renviron")
Sys.setenv(SQL_DIR = Sys.getenv("BETTR_SQL_DIR"))

withr::defer({
  message("!!! testing over; cleanup time")
  bettr::drop_table(
    connection_name = "app_dqhi_dev",
    table_name = "bettr_test_data"
  )
})

testthat::test_that("we can create a table, append data to it, pull data from it, and drop the table", {
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

    message("----- basics: we can create a table and append data to it")
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
      actual %>% dplyr::select(value_dt) %>% 
        dplyr::pull() %>% format(format = "%H:%M:%S"),
      expected %>% dplyr::select(value_dt) %>%
        dplyr::pull() %>% format(format = "%H:%M:%S")
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

testthat::test_that("We can run one or more transactions with zero, one, or many bind rows", {
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

    message("----- transactions: we can create a table and append data to it")
    result <- bettr::append_rows(
      rows = rows,
      connection_name = "app_dqhi_dev",
      table_name = "bettr_test_data",
      suppress_bind_logging = TRUE
    )

    message("----- we can append redundant data without suppressed logs")
    result <- bettr::append_rows(
      rows = rows,
      connection_name = "app_dqhi_dev",
      table_name = "bettr_test_data"
    )

    # TODO test that there are 8 rows
    testthat::expect_equal(
      8,
      tibble::tibble(value_num = 4) %>%
        bettr::get_rows(
          connection_name = "app_dqhi_dev",
          sql = "get_bettr_test_data_by_number"
        ) %>%
        dplyr::count() %>%
        dplyr::pull()
    )

    message("---- we can run a single transaction, no bind rows")
    bettr::execute_stmts(
      connection_name = "app_dqhi_dev",
      sql_file = "one_tx_no_bind_rows"
    )

    # TODO test that no records with value_num = 1 remain
    testthat::expect_equal(
      0,
      tibble::tibble(value_num = 1) %>%
        bettr::get_rows(
          connection_name = "app_dqhi_dev",
          sql = "get_bettr_test_data_by_number"
        ) %>%
        dplyr::count() %>%
        dplyr::pull()
    )

    message("---- we can run multiple transactions, no bind rows")
    bettr::execute_stmts(
      connection_name = "app_dqhi_dev",
      sql_file = "many_tx_no_bind_rows"
    )

    # TODO test that no records with value_num in (2, 4) remain
    testthat::expect_equal(
      0,
      tibble::tibble(value_num = 4) %>%
        bettr::get_rows(
          connection_name = "app_dqhi_dev",
          sql = "get_bettr_test_data_by_number"
        ) %>%
        dplyr::filter(value_num %in% c(2, 4)) %>%
        dplyr::count() %>%
        dplyr::pull()
    )

    message("---- we can run a single transaction, one bind row")
    tibble::tibble(
      value_str = "Erica",
      value_num = 5
    ) %>%
      bettr::execute_stmts(
        connection_name = "app_dqhi_dev",
        sql_file = "one_tx_one_bind_row"
      )

    # TODO test that there is one new row with value_num = 5, named Erica
    testthat::expect_equal(
      1,
      tibble::tibble(value_num = 5) %>%
        bettr::get_rows(
          connection_name = "app_dqhi_dev",
          sql = "get_bettr_test_data_by_number"
        ) %>%
        dplyr::filter(value_num == 5, value_str == "Erica") %>%
        dplyr::count() %>%
        dplyr::pull()
    )

    message("---- we can run multiple transactions, one bind row")
    tibble::tibble(
      value_str = "Francine",
      value_num = 6
    ) %>%
      bettr::execute_stmts(
        connection_name = "app_dqhi_dev",
        sql_file = "many_tx_one_bind_row",
      )

    # TODO test that there is no longer a row with value_num = 5
    testthat::expect_equal(
      0,
      tibble::tibble(value_num = 5) %>%
        bettr::get_rows(
          connection_name = "app_dqhi_dev",
          sql = "get_bettr_test_data_by_number"
        ) %>%
        dplyr::filter(value_num == 5) %>%
        dplyr::count() %>%
        dplyr::pull()
    )

    # TODO test that there is a row with value_num = 6, named Francine
    testthat::expect_equal(
      1,
      tibble::tibble(value_num = 6) %>%
        bettr::get_rows(
          connection_name = "app_dqhi_dev",
          sql = "get_bettr_test_data_by_number"
        ) %>%
        dplyr::filter(value_num == 6, value_str == "Francine") %>%
        dplyr::count() %>%
        dplyr::pull()
    )

    message("---- we can run multiple transactions, many bind rows")
    tibble::tibble(
        add_value_str = c("Georgia", "Henrietta"),
        add_value_num = c(7, 8),
        delete_value_num = c(5, 6)
      ) %>%
      bettr::execute_stmts(
        connection_name = "app_dqhi_dev",
        sql_file = "many_tx_many_bind_rows",
      )

    # TODO test that there are no longer any rows with value_num in (5, 6)
    testthat::expect_equal(
      0,
      tibble::tibble(value_num = 8) %>%
        bettr::get_rows(
          connection_name = "app_dqhi_dev",
          sql = "get_bettr_test_data_by_number"
        ) %>%
        dplyr::filter(value_num %in% c(5, 6)) %>%
        dplyr::count() %>%
        dplyr::pull()
    )

    # TODO test that there are two new rows with value_num (7, 8)
    testthat::expect_equal(
      2,
      tibble::tibble(value_num = 8) %>%
        bettr::get_rows(
          connection_name = "app_dqhi_dev",
          sql = "get_bettr_test_data_by_number"
        ) %>%
        dplyr::filter(value_num %in% c(7, 8)) %>%
        dplyr::count() %>%
        dplyr::pull()
    )
})

withr::deferred_run()