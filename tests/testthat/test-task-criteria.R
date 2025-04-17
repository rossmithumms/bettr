# 2025-04-14
# A test suite for `bettr::get_bettr_tasks_by_criteria()`,
# a function that allows client code to introspect the state
# of job refresh, so that tasks can be more informed of
# the run state of their upstream dependencies.

# SEE .Renviron.test for BETTR_* env vars used in these tests.
# Similar ones will need to be populated in Jenkinsfile for prod.

readRenviron("/workspaces/brain/.Renviron")
readRenviron("/workspaces/brain/.Renviron.test")

# This env allows this executing process to use the
# function `bettr::get_bettr_tasks_by_attributes()` to return
# test data payload and not live data.
# (Similarly, live processes besides this one that do not
# have this flag set will ignore unit test data.)
Sys.setenv("__BETTR_LIB_TEST_MODE" = "Y")

# Deletes test task rows from the BETTR_HOST's BETTR_TASK table.
# This is done between smaller tests and after all tests are done.
task_cleanup <- function() {
  message("+++ task_cleanup")
  bettr::execute_stmts(
    connection_name = "bettr_host",
    sql_file = "delete_test_bettr_rows"
  )
}

withr::defer(
  {
    task_cleanup()
  }
)

# Load a TSV for testing and validation
criteria_testdata <- testthat::test_path(
  "testdata", "test-task-criteria.tsv"
) |>
  readr::read_tsv() |>
  # dplyr::slice_head(n = 100) |>
  dplyr::rename_all(
    snakecase::to_snake_case
  ) |>
  dplyr::mutate(
    dplyr::across(dplyr::ends_with("_dt"), \(x) {
      lubridate::dmy_hms(x, tz = "America/Detroit")
    })
  )

# dplyr::glimpse(criteria_testdata)

# Use a sugar method to keep testing a little cleaner
expect_matched_rows <- function(desc, criteria_tibble, filter_func) {
  message(stringr::str_glue("========== expect_matched_rows: {desc}"))
  rs <- criteria_tibble |>
    bettr::get_bettr_tasks_by_criteria() |>
    dplyr::transmute(
      key = paste(bettr_task_job_id, bettr_task_job_priority)
    ) |>
    dplyr::arrange(key) |>
    dplyr::pull(key)

  es <- criteria_testdata |>
    filter_func() |>
    dplyr::transmute(
      key = paste(bettr_task_job_id, bettr_task_job_priority)
    ) |>
    dplyr::arrange(key) |>
    dplyr::pull(key)

  # dplyr::glimpse(rs)
  # dplyr::glimpse(es)

  # Create a compound key from the job ID and priority between
  # the two sets and ensure there is a 1:1 match
  testthat::expect_equal(
    setdiff(rs, es), as.character(c())
  )
}

testthat::test_that("all bettr::get_bettr_tasks_by_criteria() queries pass", {
  # Append this load of test rows to BETTR_TASK
  criteria_testdata |>
    bettr::append_rows(
      connection_name = "bettr_host",
      table_name = "bettr_task"
    )

  #############################################################################

  # Ensure we can get all tasks by...
  expect_matched_rows(
    "a project name",
    tibble::tibble(
      bettr_task_git_project = "etl-microbiology"
    ),
    \(x) {
      x |> dplyr::filter(
        stringr::str_detect(bettr_task_git_project, "etl-microbiology")
      )
    }
  )

  expect_matched_rows(
    "a project pattern",
    tibble::tibble(
      bettr_task_git_project = "etl-.*p"
    ),
    \(x) {
      x |> dplyr::filter(
        stringr::str_detect(bettr_task_git_project, "etl-.*p")
      )
    }
  )

  expect_matched_rows(
    "a project pattern and a comment",
    tibble::tibble(
      bettr_task_git_project = "etl-.*p",
      bettr_task_job_comment = "history"
    ),
    \(x) {
      x |> dplyr::filter(
        stringr::str_detect(bettr_task_git_project, "etl-.*p"),
        stringr::str_detect(bettr_task_job_comment, "history")
      )
    }
  )

  #############################################################################
  print("---------- querying by opt_start_dt and opt_end_dt variations")

  expect_matched_rows(
    "a single day as a start/end pair",
    tibble::tibble(
      opt_start_dt = lubridate::ymd("2025-02-28"),
      opt_end_dt = lubridate::ymd("2025-02-28")
    ),
    \(x) {
      x |> dplyr::filter(
        opt_start_dt >= lubridate::ymd("2025-02-28"),
        opt_end_dt <= lubridate::ymd("2025-02-28")
      )
    }
  )

  expect_matched_rows(
    "a range over a start/end pair",
    tibble::tibble(
      opt_start_dt = lubridate::ymd("2025-01-01"),
      opt_end_dt = lubridate::ymd("2025-02-28")
    ),
    \(x) {
      x |> dplyr::filter(
        opt_start_dt >= lubridate::ymd("2025-01-01"),
        opt_end_dt <= lubridate::ymd("2025-02-28")
      )
    }
  )

  expect_matched_rows(
    "a start date, LTE match",
    tibble::tibble(
      opt_start_dt = lubridate::ymd("2025-01-01")
    ),
    \(x) {
      x |> dplyr::filter(
        opt_start_dt >= lubridate::ymd("2025-01-01")
      )
    }
  )

  expect_matched_rows(
    "an end date, GTE match",
    tibble::tibble(
      opt_end_dt = lubridate::ymd("2025-01-01")
    ),
    \(x) {
      x |> dplyr::filter(
        opt_end_dt <= lubridate::ymd("2025-01-01")
      )
    }
  )

  #############################################################################
  print("---------- querying by opt_number_list and opt_char_list")
  expect_matched_rows(
    "an opt_number_list literal",
    tibble::tibble(
      opt_number_list = "10,-2"
    ),
    \(x) {
      x |> dplyr::filter(
        stringr::str_detect(opt_number_list, "10,-2")
      )
    }
  )

  expect_matched_rows(
    "an opt_number_list pattern",
    tibble::tibble(
      opt_number_list = "-2"
    ),
    \(x) {
      x |> dplyr::filter(
        stringr::str_detect(opt_number_list, "-2")
      )
    }
  )

  expect_matched_rows(
    "an opt_char_list literal",
    tibble::tibble(
      opt_char_list = "mumble,frotz"
    ),
    \(x) {
      x |> dplyr::filter(
        stringr::str_detect(opt_char_list, "mumble,frotz")
      )
    }
  )

  expect_matched_rows(
    "an opt_char_list pattern",
    tibble::tibble(
      opt_char_list = "o{1,2}"
    ),
    \(x) {
      x |> dplyr::filter(
        stringr::str_detect(opt_char_list, "o{1,2}")
      )
    }
  )

  #############################################################################
  print("---------- querying by statuses and errors")

  expect_matched_rows(
    "an exact status",
    tibble::tibble(
      last_status = "20"
    ),
    \(x) {
      x |> dplyr::filter(
        stringr::str_detect(last_status, "20")
      )
    }
  )

  expect_matched_rows(
    "a set of last statuses",
    tibble::tibble(
      last_status = "(10|20)"
    ),
    \(x) {
      x |> dplyr::filter(
        stringr::str_detect(last_status, "(10|20)")
      )
    }
  )

  expect_matched_rows(
    "an error pattern",
    tibble::tibble(
      last_error = "is empty"
    ),
    \(x) {
      x |> dplyr::filter(
        stringr::str_detect(last_error, "is empty")
      )
    }
  )

  #############################################################################
  print("---------- querying by expiry")

  expect_matched_rows(
    "a historical task",
    tibble::tibble(
      opt_cache_expiry_mins = 0
    ),
    \(x) {
      x |> dplyr::filter(
        as.double(opt_cache_expiry_mins) <= 0
      )
    }
  )

  expect_matched_rows(
    "any expiry task",
    tibble::tibble(
      opt_cache_expiry_mins = 1
    ),
    \(x) {
      x |> dplyr::filter(
        as.double(opt_cache_expiry_mins) >= 1
      )
    }
  )

  expect_matched_rows(
    "all with expiry > 10",
    tibble::tibble(
      opt_cache_expiry_mins = 10
    ),
    \(x) {
      x |> dplyr::filter(
        as.double(opt_cache_expiry_mins) >= 10
      )
    }
  )

  expect_matched_rows(
    "all with expiry > 60 * 24",
    tibble::tibble(
      opt_cache_expiry_mins = 60 * 24
    ),
    \(x) {
      x |> dplyr::filter(
        as.double(opt_cache_expiry_mins) >= 60 * 24
      )
    }
  )

  #############################################################################
  print("---------- testing bettr::assert_task_status()")
  # Assert succeeds, 1 matching task in data set
  assert_rs <- bettr::assert_task_status(
    tibble::tibble(
      bettr_task_git_project = "__bt_etl-cp",
      bettr_task_name = "get_soft_orders",
      opt_start_dt = lubridate::ymd("2024-02-01"),
      opt_end_dt = lubridate::ymd("2024-02-01"),
      last_status = "30"
    )
  )

  testthat::expect_true(assert_rs)

  # Assert fails, no matching tasks in data set
  tryCatch({
    assert_rs <- bettr::assert_task_status(
      tibble::tibble(
        bettr_task_git_project = "__bt_etl-cp",
        bettr_task_name = "get_soft_orders",
        opt_start_dt = lubridate::ymd("2024-02-01"),
        opt_end_dt = lubridate::ymd("2024-02-01"),
        last_status = "20"
      )
    )

    # This should never run; the error stops it
    testthat::expect_true(FALSE)
  },
  error = \(err) {
    message("+++ bettr::assert_task_status() errored successfully")
    testthat::expect_true(TRUE)
  })

  # Assert succeeds, 2 matching tasks in dataset
  # (In this case, last ran failed; first ran succeeded)
  did_warn <<- FALSE

  tryCatch(
    {
      assert_rs <- bettr::assert_task_status(
        tibble::tibble(
          bettr_task_git_project = "__bt_etl-phlebotomy",
          bettr_task_name = "init_configs",
          last_status = "20"
        )
      )
    },

    warning = \(warn) {
      message("+++ bettr::assert_task_status() warned successfully")
      did_warn <<- TRUE
    },

    error = \(err) {
      message(err)
      message("!!! Marking expectation as failed")
      testthat::expect_true(FALSE)
    },

    finally = {
      testthat::expect_true(did_warn)
    }
  )

  # Assert fails, 2 matching tasks in dataset
  # (In this case, last ran succeeded; first never started)
  did_warn <<- FALSE

  tryCatch(
    {
      assert_rs <- bettr::assert_task_status(
        tibble::tibble(
          bettr_task_git_project = "__bt_etl-phlebotomy",
          bettr_task_name = "get_wiw_data",
          last_status = "0"
        )
      )

      # This should never run; the error stops it
      testthat::expect_true(FALSE)
    },

    warning = \(warn) {
      message("+++ bettr::assert_task_status() warned successfully")
      did_warn <<- TRUE
    },

    error = \(err) {
      message("+++ bettr::assert_task_status() errored successfully")
      testthat::expect_true(TRUE)
    },

    finally = {
      testthat::expect_true(did_warn)
    }
  )
})

withr::deferred_run()