readRenviron("/workspaces/brain/.Renviron")
Sys.setenv(SQL_DIR = Sys.getenv("BETTR_SQL_DIR"))

test_that("a simple parameterized query returns results", {
    actual <- tibble::tibble(
      value_str = c("ANNE", "BETSY"),
      value_nbr = c(1, 2)
    )

    expected <- tibble::tibble(value_nbr = 2) %>%
      bettr::get_rows(
        connection_name = "app_dqhi_dev",
        sql = "get_bettr_test_data_by_number"
      ) %>%
      tibble::tibble()

    testthat::expect_equal(
      actual %>% dplyr::pull(),
      expected %>% dplyr::pull()
    )
  }
)
