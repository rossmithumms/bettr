#
# 2024-03-18
# Pseudo-code for bettr's job logic.

# bettr::init() calls the logic stored in this function.

# 1. Using the link below as reference, search the parent
# directory tree for the first "sql" directory and configure
# bettr to use it:
# https://stackoverflow.com/questions/67791408/in-r-how-to-find-a-file-located-in-any-parent-directory-upper-than-working-dire

# 2. The command line arguments are parsed and interpreted,
# similar to how project-specific command header files are
# doing this.  Bringing this logic into bettr::call() will
# standardize this logic across projects, create reuse,
# and reduce versioning challenges.
# In this step, the task input options are validated (the terms
# are well-formed, they represent tasks that haven't been
# completed yet according to a persistent and standardized
# task log table) and, if all is well, the task is created.

# 3. bettr::init() returns a KVP of options to configure the
# task for the job.  It will return some other value or exit
# the R script entirely if the job has already been completed
# and should not be run.

# The objective of this approach is to rely, as much as
# possible, on the job table FIRST to populate data tables
# via ETL.  You can run commands on their own and pass direct
# arguments for them to use for a refresh, but if they match
# up to an already-run task in the task table, the task just
# won't run.  In this way, the job table is the main to-do
# list for all ETL work done with bettr, as well as the log
# of completed tasks and their last run state.

# 2024-03-20

# Let's stub out this code.

# TOC
# bettr::init(): creates connections, inspects the job table,
# interprets CLI options, and passes appropriate parameters
# to the job function (or exits if there is nothing to do).
# Also sets up exit handling functions to clean up persistent
# connections to the job server.
# 
# bettr::run_job(): the wrapper that accepts a job function.

# bettr::init(): Initializes the job with a task. Called
# inside run_job().

# Check command line options.  Were any passed?

# Check the call to run_job().

# If so, check if there is a matching row in the job table.
# Check the row's fulfillment status.

# If it was completed, and there is no CLI option to force
# a rerun, write a message to that effect and exit (no error).

# If it was completed, and there is a CLI option to force
# a rerun, go ahead and rerun.

# For other statuses, handle appropriately.  For now, we will
# assume other statuses are "not started", "in progress",
# and "error".  "in progress" should never be rerun, and
# if it's left hanging in the system, needs a reset.

# If not, pull the next available task from the task table
# in priority order, given the job name.

readRenviron(".Renviron")
readRenviron(".Renviron.test")

Sys.setenv(SQL_DIR = "/workspaces/brain/lib/bettr/sql")

library(callr)
library(bettr)

#' Send a Heartbeat to the Bettr Job Host
#' 
#' This function updates BETTR_TASK.LAST_HB_DT in the "Bettr
#' host" Oracle database.
#' @param bettr_task_id Primary key of the row to update.
#' @return nothing
#' 
do_heartbeat <- function(bettr_task_id) {
  # TODO write to a heartbeat log table, for testing
  tibble::tibble(
    bettr_task_id = bettr_task_id,
    last_hb_dt = lubridate::now()
  ) |>
    bettr::execute_stmts(
      connection_name = "bettr_host",
      sql = "update_bettr_task_last_hb_dt"
    )
}

long_job <- function() {
  print("... starting long_job")
  Sys.sleep(10)
  tibble::tibble(
    value_str = "foo",
    value_num = 1L,
    value_dt = lubridate::now()
  ) |>
    bettr::append_rows(
      connection_name = "app_dqhi_dev",
      table_name = "bettr_test_data"
    )
  print("+++ long_job complete")
}

# wrapper_01 <- function() {
#   heartbeat_rp <<- callr::r_bg(do_heartbeat)
#   on.exit(heartbeat_rp$kill, add = TRUE)
#   callr::r(long_job)
# }

wrapper <- function(job) {
  rs_task <- callr::r_session$new()
  rs_hb <- callr::r_session$new()
  on.exit({
    rs_task$close()
    rs_hb$close()
  }, add = TRUE)
  rs_task$call(job)
  state <- "timeout"
  while (state != "ready") {
    rs_hb$run(do_heartbeat)
    cat(".")
    state <- rs_task$poll_process(timeout = 3000)
  }
  #message(rs_task$read()$stdout)
  cat("\n")
  print(rs_task$read())
}

# When the wrapper calls a job that connects to a
# database and takes some time, it can result in
# multiple heartbeat calls, which is what we want.
wrapper(job = long_job)

broken_job <- function() {
  print("... starting broken_job")
  tibble::tibble(
    value_str = "foo"
  ) |>
    bettr::append_rows(
      connection_name = "nope",
      table_name = "bettr_test_data"
    )
  print("+++ broken_job complete")
}

# When the wrapper calls a job that throws an error
# and fails, the heartbeat is still called and
# logged, which is what we want to happen.
wrapper(job = broken_job)
