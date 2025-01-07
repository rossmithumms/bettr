# 2024-03-22
# We're ready to start building out tests for the new task logic.

# SEE .Renviron.test for BETTR_* env vars used in these tests.
# Similar ones will need to be populated in Jenkinsfile for prod.

readRenviron("/workspaces/brain/.Renviron")
readRenviron("/workspaces/brain/.Renviron.test")

# Deletes test task rows from the BETTR_HOST's BETTR_TASK table.
# This is done between smaller tests and after all tests are done.
task_cleanup <- function() {
  message("+++ task_cleanup")
  bettr::execute_stmts(
    connection_name = "bettr_host",
    sql_file = "delete_test_bettr_rows"
  )
}

# Runs a script to drop a test table used by some of the tasks
# that are part of testing this logic.
# This is done when, subject to the task and test logic,
# we need to be able to reinitialize the table without error.
table_cleanup <- function() {
  message("+++ table_cleanup")
  bettr::execute_stmts(
    connection_name = "app_dqhi_dev",
    sql_file = "drop_bettr_task_test"
  )
}

testthat::teardown(
  {
    task_cleanup()
  }
)

testthat::test_that("all bettr tests pass", {
  #############################################################################
  print("---------- add jobs to the bettr host")

  added_tasks <- tibble::tibble(
    bettr_task_git_project = c("bettr", "bettr", "bettr"),
    bettr_task_git_branch = c("feature/task", "feature/task", "feature/task"),
    bettr_task_name = c("task_test_before", "task_test_error_during", "task_test_after"),
    bettr_task_job_comment = c("__TEST__", "__TEST__", "__TEST__"),
    bettr_task_job_priority = c(1, 1, 1)
  )

  added_tasks |> bettr::add_job_to_host()

  #############################################################################
  print("---------- get_next_bettr_task returns sorted list of all pending tasks")

  bettr_task <- tibble::tibble(
    bettr_task_git_project = "bettr",
    bettr_task_git_branch = "feature/task",
    incl_live_refresh = 1
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

  #############################################################################
  print("---------- get_next_bettr_job returns sorted list of all tasks in only the next job")

  bettr_job <- tibble::tibble(
    bettr_task_git_project = "bettr",
    bettr_task_git_branch = "feature/task",
    incl_live_refresh = 1
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

  #############################################################################
  print("---------- run_next_task_in_queue runs first task in first job")
  # Run the top task in the stack; verify that it was the
  # task `task_test_before` and that 1 row was created in its
  # newly-minted table

  task_result <- bettr::run_next_task_in_queue(
    project = "bettr",
    branch = "feature/task",
    return_result = TRUE
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
  task_result <- bettr::run_next_task_in_queue(
    project = "bettr",
    branch = "feature/task",
    return_result = TRUE
  )

  task_failures <- bettr::get_rows(
    connection_name = "bettr_host",
    sql = "get_failed_bettr_tasks"
  )

  # TODO verify that task_result's error matches task_failures

  # Because this task is designed to fail, we need to clean out
  # the current test tasks and refresh with ones that will
  # process successfully.
  # Delete them now and set up a new set of tasks for testing.
  task_cleanup()
  table_cleanup()

  #############################################################################
  print("---------- run_next_job_in_queue runs all tasks in next job successfully")

  added_tasks <- tibble::tibble(
    bettr_task_git_project = c("bettr", "bettr"),
    bettr_task_git_branch = c("feature/task", "feature/task"),
    bettr_task_name = c("task_test_before", "task_test_update_foo"),
    bettr_task_job_comment = c("__TEST__", "__TEST__"),
    bettr_task_job_priority = c(1, 1)
  )

  added_tasks |> bettr::add_job_to_host()

  task_results <- bettr::run_next_job_in_queue(return_result = TRUE)

  testthat::expect_equal(dplyr::count(task_results) |> as.double(), 2)

  testthat::expect_equal(
    task_results$bettr_task_name[1],
    added_tasks$bettr_task_name[1]
  )

  testthat::expect_equal(
    task_results$bettr_task_name[2],
    added_tasks$bettr_task_name[2]
  )

  task_test_rows <- bettr::get_rows(
    connection_name = "app_dqhi_dev",
    sql = "get_bettr_task_test"
  )

  testthat::expect_equal(task_test_rows$foo[1] %>% as.double(), 3)
  testthat::expect_equal(task_test_rows$bar[1] %>% as.double(), 2)
  testthat::expect_equal(
    task_test_rows$current_dt[1] %>% format("%Y-%m-%d"),
    lubridate::today() %>% format("%Y-%m-%d")
  )

  task_cleanup()
  table_cleanup()

  #############################################################################
  print("---------- run_next_job_in_queue skips jobs with only live refresh tasks")

  tibble::tibble(
    bettr_task_git_project = "bettr",
    bettr_task_git_branch = "feature/task",
    bettr_task_name = "task_test_update_foo",
    bettr_task_job_comment = "__TEST__",
    bettr_task_job_priority = 1,
    opt_cache_expiry_mins = 10
  ) |>
    bettr::add_job_to_host()

  tibble::tibble(
    bettr_task_git_project = "bettr",
    bettr_task_git_branch = "feature/task",
    bettr_task_name = "task_test_before",
    bettr_task_job_comment = "__TEST__",
    bettr_task_job_priority = 1,
    opt_cache_expiry_mins = -1
  ) |>
    bettr::add_job_to_host()

  bettr::run_next_job_in_queue(incl_live_refresh = FALSE)

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

  task_cleanup()
  table_cleanup()

  #############################################################################
  print("---------- caches exhaust when all are run, and rerun after expiry")

  added_tasks <- tibble::tibble(
    bettr_task_git_project = c("bettr", "bettr", "bettr"),
    bettr_task_git_branch = c("feature/task", "feature/task", "feature/task"),
    bettr_task_name = c("task_test_before", "task_test_after", "task_test_delete"),
    bettr_task_job_comment = c("__TEST__", "__TEST__", "__TEST__"),
    bettr_task_job_priority = c(1, 1, 1),
    opt_cache_expiry_mins = c(-1, 1, -1)
  )

  added_tasks %>% bettr::add_job_to_host()

  # Run the first two tasks immediately; keep the 2nd result for testing
  bettr::run_next_task_in_queue(
    project = "bettr",
    branch = "feature/task",
  )

  task_result_1 <- bettr::run_next_task_in_queue(
    project = "bettr",
    branch = "feature/task",
    return_result = TRUE
  )

  # Wait out the cache expiry on the task
  message("... sleeping for 75 seconds ...")
  Sys.sleep(75)
  message("... naptime's over!")

  # Run the next test, which should just be `task_test_after` again
  task_result_2 <- bettr::run_next_task_in_queue(
    project = "bettr",
    branch = "feature/task",
    return_result = TRUE
  )

  # Make sure both executions yield the same result
  testthat::expect_equal(
    task_result_1$result$value, task_result_2$result$value
  )

  # Now run the final task in the queue, task_test_delete
  task_result_3 <- bettr::run_next_task_in_queue(
    project = "bettr",
    branch = "feature/task",
    return_result = TRUE
  )

  # It should just return TRUE
  testthat::expect_equal(
    task_result_3$rs_result$result$value,
    "task_test_delete"
  )

  # Last, try to run another task before caches expire.
  # Nothing should run/task exhaustion.
  task_result_4 <- bettr::run_next_task_in_queue(
    project = "bettr",
    branch = "feature/task",
    return_result = TRUE
  )

  testthat::expect_null(task_result_4)
})