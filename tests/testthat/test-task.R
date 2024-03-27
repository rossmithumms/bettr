# 2024-03-22
# We're ready to start building out tests for the new task logic.

# SEE .Renviron.test for BETTR_* env vars used in these tests.
# Similar ones will need to be populated in Jenkinsfile for prod.

readRenviron("/workspaces/brain/.Renviron")
readRenviron("/workspaces/brain/.Renviron.test")

test_cleanup <- function(do_cleanup = TRUE) {
  if (do_cleanup) {
    message("!!! cleanup time")
    bettr::execute_stmts(
      connection_name = "bettr_host",
      sql_file = "delete_test_bettr_rows_and_tables"
    )
  }
}

withr::defer({
  test_cleanup(Sys.getenv("BETTR_TASK_TEST_CLEANUP_AFTER") == 1L)
})

testthat::test_that("add jobs to the bettr host", {
  added_tasks <- tibble::tibble(
    bettr_task_git_project = c("bettr", "bettr", "bettr"),
    bettr_task_git_branch = c("feature/task", "feature/task", "feature/task"),
    bettr_task_name = c("task_test_before", "task_test_error_during", "task_test_after"),
    bettr_task_job_comment = c("__TEST__", "__TEST__", "__TEST__"),
    bettr_task_job_priority = c(1, 1, 1),
    opt_cache_expiry_mins = c(120, 120, 120)
  )

  added_tasks %>%
    bettr::add_job_to_host()

  bettr_task <- tibble::tibble(
    bettr_task_git_project = "bettr",
    bettr_task_git_branch = "feature/task"
  ) %>%
    bettr::get_rows(
      connection_name = "bettr_host",
      sql = "get_next_bettr_task"
    )

  testthat::expect_equal(
    bettr_task %>% dplyr::count() %>% as.double(), 3
  )

  testthat::expect_equal(
    bettr_task$bettr_task_name[1], "task_test_before"
  )

  testthat::expect_equal(
    bettr_task$bettr_task_name[2], "task_test_error_during"
  )

  testthat::expect_equal(
    bettr_task$bettr_task_name[3], "task_test_after"
  )
})

# Run the top task in the stack; verify that it was the
# task `task_test_before` and that 1 row was created in its
# newly-minted table
testthat::test_that("runs the tasks with reporting and error handling", {
  # task_test_before
  task_result <- bettr::run_next_task_in_queue(
    project = "bettr",
    branch = "feature/task"
  )

  testthat::expect_equal(
    c("rs_result", "bettr_task"),
    names(task_result)
  )

  # Generate a result report JSON manually
  expected_json <- task_result$rs_result$result$value %>%
    jsonlite::toJSON() %>%
    as.character()

  # Make sure the result is similarly stored on the task row
  task_from_db <- tibble::tibble(
    bettr_task_key = task_result$bettr_task$bettr_task_key
  ) %>%
    bettr::get_rows(
      connection_name = "bettr_host",
      sql = "get_bettr_task_by_id"
    )

  testthat::expect_equal(
    task_from_db$last_report, expected_json
  )

  # Now that we're done testing the reporting features,
  # let's test that the task actually impacted the data environment
  # the way we expected.
  task_test_rows <- bettr::get_rows(
    connection_name = "app_dqhi_dev",
    sql = "get_bettr_task_test"
  )

  testthat::expect_equal(task_test_rows$foo[1] %>% as.double(), 1)
  testthat::expect_equal(task_test_rows$bar[1] %>% as.double(), 2)
  testthat::expect_equal(
    task_test_rows$current_dt[1] %>% format("%Y-%m-%d"),
    lubridate::today() %>% format("%Y-%m-%d")
  )

  # task_test_error_during
  bettr::run_next_task_in_queue(
    project = "bettr",
    branch = "feature/task"
  )

  task_failures <- bettr::get_rows(
    connection_name = "bettr_host",
    sql = "get_failed_bettr_tasks"
  )

  dplyr::glimpse(task_failures)
})

# TODO verify that the updated state of the tasks in the
# table reflects reality (completed)

# TODO run the top task in the stack; verify that nothing
# ran, because there are no more jobs

# TODO write out more logic to handle expiry, then write
# tests to verify expiry behavior/constant refresh
# behaves as anticipated