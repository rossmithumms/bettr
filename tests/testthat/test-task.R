# 2024-03-22
# We're ready to start building out tests for the new task logic.

Sys.setenv(
  BETTR_TASK_GIT_PROJECT = "bettr",
  BETTR_TASK_GIT_BRANCH = "feature/task",
  BETTR_TASK_GIT_COMMIT = "12345"
)

withr::defer({
  message("!!! testing over; cleanup time")
  # bettr::execute_stmts(
  #   connection_name = "bettr_host",
  #   sql_file = "delete_test_bettr_tasks"
  # )
})

testthat::test_that("successfully add jobs to the bettr host", {
  tibble::tibble(
    bettr_task_git_project = c("bettr", "bettr"),
    bettr_task_git_branch = c("feature/task", "feature/task"),
    bettr_task_name = c("task_test_before", "task_test_after"),
    bettr_task_job_comment = c("__TEST__", "__TEST__"),
    bettr_task_job_priority = c(1, 1),
    opt_cache_expiry = c(120, 120)
  ) %>%
    bettr::add_job()
})

# TODO run the top task in the stack; verify that it was the
# task `task_test_before` and that 1 row was created in its
# newly-minted table
testthat::test_that("successfully runs the next expected task", {
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