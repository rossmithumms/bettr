# 2024-03-22
# We're ready to start building out tests for the new task logic.

# SEE .Renviron.test for BETTR_* env vars used in these tests.
# Similar ones will need to be populated in Jenkinsfile for prod.

withr::defer({
  message("!!! testing over; cleanup time")
  bettr::execute_stmts(
    connection_name = "bettr_host",
    sql_file = "delete_test_bettr_rows_and_tables"
  )
})

testthat::test_that("add jobs to the bettr host", {
  tibble::tibble(
    bettr_task_git_project = c("bettr", "bettr"),
    bettr_task_git_branch = c("feature/task", "feature/task"),
    bettr_task_name = c("task_test_before", "task_test_after"),
    bettr_task_job_comment = c("__TEST__", "__TEST__"),
    bettr_task_job_priority = c(1, 1),
    opt_cache_expiry_mins = c(120, 120)
  ) %>%
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
    bettr_task %>% dplyr::count() %>% as.double(), 2
  )

  testthat::expect_equal(
    bettr_task$bettr_task_name[1], "task_test_before"
  )

  testthat::expect_equal(
    bettr_task$bettr_task_name[2], "task_test_after"
  )
})

# Run the top task in the stack; verify that it was the
# task `task_test_before` and that 1 row was created in its
# newly-minted table
testthat::test_that("runs the next expected task", {
  bettr::run_next_task_in_queue(
    project = "bettr",
    branch = "feature/task"
  )
})

# TODO verify that the updated state of the task in the
# table reflects reality (completed)

# TODO run the top task in the stack; verify that it did
# alter the world as intended

# TODO verify that the updated state of hte task in the
# table reflects reality (completed)

# TODO run the top task in the stack; verify that nothing
# ran, because there are no more jobs

# TODO write out more logic to handle expiry, then write
# tests to verify expiry behavior/constant refresh
# behaves as anticipated