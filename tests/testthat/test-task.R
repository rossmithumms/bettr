# 2024-03-22
# We're ready to start building out tests for the new task logic.

# SEE .Renviron.test for BETTR_* env vars used in these tests.
# Similar ones will need to be populated in Jenkinsfile for prod.

readRenviron("/workspaces/brain/.Renviron")
readRenviron("/workspaces/brain/.Renviron.test")

test_cleanup <- function(do_cleanup = TRUE) {
  bettr::execute_stmts(
    connection_name = "bettr_host",
    sql_file = "delete_test_bettr_rows"
  )
  bettr::close_db_conn_pool()
}

withr::defer({
  try(
    {
      test_cleanup(Sys.getenv("BETTR_TASK_TEST_CLEANUP_AFTER") == 1L)
      bettr::execute_stmts(
        connection_name = "app_dqhi_dev",
        sql_file = "drop_bettr_task_test"
      )
    }
  )
})

testthat::test_that("add jobs to the bettr host", {
  added_tasks <- tibble::tibble(
    bettr_task_git_project = c("bettr", "bettr", "bettr"),
    bettr_task_git_branch = c("feature/task", "feature/task", "feature/task"),
    bettr_task_name = c("task_test_before", "task_test_error_during", "task_test_after"),
    bettr_task_job_comment = c("__TEST__", "__TEST__", "__TEST__"),
    bettr_task_job_priority = c(1, 1, 1)
  )

  added_tasks |> bettr::add_job_to_host()

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

  # Test again, this time with get_next_bettr_job.
  # The result should be the same.
  bettr_job <- tibble::tibble(
    bettr_task_git_project = "bettr",
    bettr_task_git_branch = "feature/task"
  ) %>%
    bettr::get_rows(
      connection_name = "bettr_host",
      sql = "get_next_bettr_job"
    )

  testthat::expect_equal(
    bettr_job %>% dplyr::count() %>% as.double(), 3
  )

  testthat::expect_equal(
    bettr_job$bettr_task_name[1], "task_test_before"
  )

  testthat::expect_equal(
    bettr_job$bettr_task_name[2], "task_test_error_during"
  )

  testthat::expect_equal(
    bettr_job$bettr_task_name[3], "task_test_after"
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

  # The command that just ran must return an rs_result (with useful
  # result and result$value properties) and a bettr_task (the task
  # input values, with updated last_state and last_error metadata)
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

  # Clean up so we can test task exhaustion and expiry next
  test_cleanup()
})

testthat::test_that("caches exhaust when all are run, and rerun after expiry", {
  added_tasks <- tibble::tibble(
    bettr_task_git_project = c("bettr", "bettr"),
    bettr_task_git_branch = c("feature/task", "feature/task"),
    bettr_task_name = c("task_test_after", "task_test_delete"),
    bettr_task_job_comment = c("__TEST__", "__TEST__"),
    bettr_task_job_priority = c(1, 1),
    opt_cache_expiry_mins = c(2, -1)
  )

  added_tasks %>% bettr::add_job_to_host()

  # Run the 1 task immediately
  task_result_1 <- bettr::run_next_task_in_queue(
    project = "bettr",
    branch = "feature/task"
  )

  # Wait out the cache expiry on the task
  message("... sleeping for 3 minutes ...")
  Sys.sleep(180)
  message("... naptime's over!")

  # Run the next test, which should just be `task_test_after` again
  task_result_2 <- bettr::run_next_task_in_queue(
    project = "bettr",
    branch = "feature/task"
  )

  # Make sure both executions yield the same result
  testthat::expect_equal(
    task_result_1$result$value, task_result_2$result$value
  )

  # Now run the final task in the queue, task_test_delete
  task_result_3 <- bettr::run_next_task_in_queue(
    project = "bettr",
    branch = "feature/task"
  )

  # It should just return TRUE
  testthat::expect_equal(
    task_result_3$rs_result$result$value,
    list(TRUE)
  )

  # Last, try to run another task before caches expire.
  # Nothing should run/task exhaustion.
  task_result_4 <- bettr::run_next_task_in_queue(
    project = "bettr",
    branch = "feature/task"
  )

  testthat::expect_null(task_result_4)
})