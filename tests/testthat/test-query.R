readRenviron("/workspaces/brain/.Renviron")
readRenviron("/workspaces/brain/.Renviron.test")
Sys.setenv(SQL_DIR = Sys.getenv("BETTR_SQL_DIR"))
Sys.setenv(ENVIRONMENT = "btestenv")
test_tz <- Sys.getenv("TZ")

withr::defer(
  {
    tryCatch(
      {
        bettr::drop_table(
          connection_name = "app_dqhi_dev",
          table_name = "bettr_test_data"
        )
      },
      error = \(e) {}
    )
    tryCatch(
      {
        bettr::execute_stmts(
          connection_name = "app_dqhi_dev",
          sql_file = "drop_bettr_init_sql"
        )
      },
      error = \(e) {}
    )
    # tryCatch(
    #   {
    #     bettr::execute_stmts(
    #       connection_name = "app_dqhi_dev",
    #       sql_file = "drop_bettr_cache_test"
    #     )
    #   },
    #   error = \(e) {}
    # )
  }
)

testthat::test_that(
  "we can create a table, append/pull data, and drop the table",
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

    message("----- basics: we can create a table and append data to it")
    result <- bettr::append_rows(
      rows = rows,
      connection_name = "app_dqhi_dev",
      table_name = "bettr_test_data",
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
      sql = "get_bettr_test_data"
    ) |>
      tibble::tibble()

    testthat::expect_equal(
      rows |> dplyr::summarise(n = dplyr::n()),
      expected |> dplyr::summarise(n = dplyr::n())
    )

    expected <- tibble::tibble(value_num = 2) |>
      bettr::get_rows(
        connection_name = "app_dqhi_dev",
        sql = "get_bettr_test_data_by_number"
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
      table_name = "bettr_test_data"
    )
  }
)

testthat::test_that(
  "We can run one or more transactions with zero, one, or many bind rows",
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

    # test that there are 8 rows
    testthat::expect_equal(
      8,
      tibble::tibble(value_num = 4) |>
        bettr::get_rows(
          connection_name = "app_dqhi_dev",
          sql = "get_bettr_test_data_by_number"
        ) |>
        dplyr::count() |>
        dplyr::pull()
    )

    message("---- we can run a single transaction, no bind rows")
    bettr::execute_stmts(
      connection_name = "app_dqhi_dev",
      sql_file = "one_tx_no_bind_rows"
    )

    # test that no records with value_num = 1 remain
    testthat::expect_equal(
      0,
      tibble::tibble(value_num = 1) |>
        bettr::get_rows(
          connection_name = "app_dqhi_dev",
          sql = "get_bettr_test_data_by_number"
        ) |>
        dplyr::count() |>
        dplyr::pull()
    )

    message("---- we can run multiple transactions, no bind rows")
    bettr::execute_stmts(
      connection_name = "app_dqhi_dev",
      sql_file = "many_tx_no_bind_rows"
    )

    # test that no records with value_num in (2, 4) remain
    testthat::expect_equal(
      0,
      tibble::tibble(value_num = 4) |>
        bettr::get_rows(
          connection_name = "app_dqhi_dev",
          sql = "get_bettr_test_data_by_number"
        ) |>
        dplyr::filter(value_num %in% c(2, 4)) |>
        dplyr::count() |>
        dplyr::pull()
    )

    message("---- we can run a single transaction, one bind row")
    tibble::tibble(
      value_str = "Erica",
      value_num = 5
    ) |>
      bettr::execute_stmts(
        connection_name = "app_dqhi_dev",
        sql_file = "one_tx_one_bind_row"
      )

    # test that there is one new row with value_num = 5, named Erica
    testthat::expect_equal(
      1,
      tibble::tibble(value_num = 5) |>
        bettr::get_rows(
          connection_name = "app_dqhi_dev",
          sql = "get_bettr_test_data_by_number"
        ) |>
        dplyr::filter(value_num == 5, value_str == "Erica") |>
        dplyr::count() |>
        dplyr::pull()
    )

    message("---- we can run multiple transactions, one bind row")
    tibble::tibble(
      value_str = "Francine",
      value_num = 6
    ) |>
      bettr::execute_stmts(
        connection_name = "app_dqhi_dev",
        sql_file = "many_tx_one_bind_row",
      )

    # test that there is no longer a row with value_num = 5
    testthat::expect_equal(
      0,
      tibble::tibble(value_num = 5) |>
        bettr::get_rows(
          connection_name = "app_dqhi_dev",
          sql = "get_bettr_test_data_by_number"
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
          sql = "get_bettr_test_data_by_number"
        ) |>
        dplyr::filter(value_num == 6, value_str == "Francine") |>
        dplyr::count() |>
        dplyr::pull()
    )

    message("---- we can run multiple transactions, many bind rows")
    tibble::tibble(
      add_value_str = c("Georgia", "Henrietta"),
      add_value_num = c(7, 8),
      delete_value_num = c(5, 6)
    ) |>
      bettr::execute_stmts(
        connection_name = "app_dqhi_dev",
        sql_file = "many_tx_many_bind_rows",
      )

    # test that there are no longer any rows with value_num in (5, 6)
    testthat::expect_equal(
      0,
      tibble::tibble(value_num = 8) |>
        bettr::get_rows(
          connection_name = "app_dqhi_dev",
          sql = "get_bettr_test_data_by_number"
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
          sql = "get_bettr_test_data_by_number"
        ) |>
        dplyr::filter(value_num %in% c(7, 8)) |>
        dplyr::count() |>
        dplyr::pull()
    )
  }
)

testthat::test_that(
  "We can generate working initialization SQL with views and archive tables",
  {

    # Get some test test data
    rows <- tibble::tibble(
      this = c("that", "other"),
      foo = c("boo", "bar"),
      mumble = c(1, 2),
      frotz = c(lubridate::now(), lubridate::now() - lubridate::days(2))
    )

    # Generate init SQL file
    rows |>
      bettr::generate_init_sql(
        connection_name = "app_dqhi_dev",
        table_name = "bettr_init_sql",
        index_columns = c(
          "foo",
          "mumble"
        )
      )

    # Ensure the table exists
    rows |>
      bettr::ensure_table(
        connection_name = "app_dqhi_dev",
        table_name = "bettr_init_sql"
      )

    # Append rows
    rows |>
      bettr::append_rows(
        connection_name = "app_dqhi_dev",
        table_name = "bettr_init_sql"
      )

    # Query the union view to ensure we see results
    rs <- bettr::get_rows(
      connection_name = "app_dqhi_dev",
      sql = "get_all_bettr_init_sql"
    )

    # Should have 2 rows
    testthat::expect_equal(
      as.double(dplyr::count(rs)), 2
    )

    # Flush to archive
    bettr::flush_to_archive(
      connection_name = "app_dqhi_dev",
      table_name = "bettr_init_sql",
      stale_column = "frotz",
      stale_dt = lubridate::now() - lubridate::days(1)
    )

    # Query the archive view to ensure we see results
    rs <- bettr::get_rows(
      connection_name = "app_dqhi_dev",
      sql = "get_arch_bettr_init_sql"
    )

    # Should have 1 row
    testthat::expect_equal(
      as.double(dplyr::count(rs)), 1
    )

    # Teardown
    bettr::execute_stmts(
      connection_name = "app_dqhi_dev",
      sql_file = "drop_bettr_init_sql"
    )
  }
)

testthat::test_that(
  "We can rotate a cached version of a view through replicates",
  {

    # Make some test test data
    rows <- tibble::tibble(
      this = c("that", "other"),
      foo = c("boo", "bar"),
      mumble = c(1, 2),
      frotz = c(lubridate::now(), lubridate::now() - lubridate::days(2))
    )

    # Generate init SQL file
    rows |>
      bettr::generate_init_sql(
        connection_name = "app_dqhi_dev",
        table_name = "bettr_cache_test",
        index_columns = c(
          "foo",
          "mumble"
        )
      )

    # Ensure the table exists
    rows |>
      bettr::ensure_table(
        connection_name = "app_dqhi_dev",
        table_name = "bettr_cache_test"
      )

    # Append rows
    rows |>
      bettr::append_rows(
        connection_name = "app_dqhi_dev",
        table_name = "bettr_cache_test"
      )

    # Initialize a view on this data; this is what we will cache
    bettr::execute_stmts(
      connection_name = "app_dqhi_dev",
      sql_file = "init_v_my_cache_test"
    )

    # ===== FIRST ROTATION
    bettr::rotate_cache(
      connection_name = "app_dqhi_dev",
      view_name = "v_my_cache_test"
    )

    # Query the cache view; do we see the expected rows?
    cached_rows <- bettr::get_rows(
      connection_name = "app_dqhi_dev",
      sql = "get_my_cache_test"
    )

    testthat::expect_equal(
      cached_rows |> dplyr::count() |> as.double(),
      2
    )

    # Add more rows to the live table
    rows |>
      bettr::append_rows(
        connection_name = "app_dqhi_dev",
        table_name = "bettr_cache_test"
      )

    # Query the cache view; same as before?
    cached_rows <- bettr::get_rows(
      connection_name = "app_dqhi_dev",
      sql = "get_my_cache_test"
    )

    testthat::expect_equal(
      cached_rows |> dplyr::count() |> as.double(),
      2
    )

    # ===== SECOND ROTATION
    # This time, use a view grant
    # (Note that this role must exist in the Oracle DB)
    bettr::rotate_cache(
      connection_name = "app_dqhi_dev",
      view_name = "v_my_cache_test",
      view_grants = c("PHLEB_ROLE")
    )

    # Query the cache view; do we see the rows added to live?
    cached_rows <- bettr::get_rows(
      connection_name = "app_dqhi_dev",
      sql = "get_my_cache_test"
    )

    testthat::expect_equal(
      cached_rows |> dplyr::count() |> as.double(),
      4
    )

    # Add more rows to the live table
    rows |>
      bettr::append_rows(
        connection_name = "app_dqhi_dev",
        table_name = "bettr_cache_test"
      )

    # Query the cache view; same as before?
    cached_rows <- bettr::get_rows(
      connection_name = "app_dqhi_dev",
      sql = "get_my_cache_test"
    )

    testthat::expect_equal(
      cached_rows |> dplyr::count() |> as.double(),
      4
    )

    # ===== THIRD ROTATION
    bettr::rotate_cache(
      connection_name = "app_dqhi_dev",
      view_name = "v_my_cache_test"
    )

    # Query the cache view; do we see the rows added to live?
    cached_rows <- bettr::get_rows(
      connection_name = "app_dqhi_dev",
      sql = "get_my_cache_test"
    )

    testthat::expect_equal(
      cached_rows |> dplyr::count() |> as.double(),
      6
    )

    # Teardown
    bettr::execute_stmts(
      connection_name = "app_dqhi_dev",
      sql_file = "drop_bettr_cache_test"
    )
  }
)