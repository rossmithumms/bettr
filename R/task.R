#' Defines Default Bettr Task Column Names and Types
#' @return A tibble with column names and values reflecting
#' the default values for those columns.  Note that several
#' are from the environment and are thus contextual defaults.
#' This object is used when populating new tasks to the host,
#' and it's assumed that the calls to `bettr::add_job()` will
#' either configure those explicitly in their bettr_task tibbles,
#' or the function will be called in an interactive session where
#' these values are set before the function is called.
#'
#' Brief definitions for each column are given below.  Please refer
#' to function documentation in this file to understand how each
#' property is interpreted.
#'
#' - bettr_task_git_project: Required.  Name of the git repository
#'   where the task R file is stored. (e.g., `phlebotomy`).
#' - bettr_task_git_branch: Required.  Name of the git branch where
#'   the source code is stored.
#' - bettr_task_git_commit: Optional.  Hash identifier of the git
#'   commit matching the task R file.  This is intended for auditing
#'   and validating only.
#' - bettr_task_job_comment: Optional.  A freetext field containing
#'   maintenance notes.
#' - bettr_task_job_id: Metadata.  A unique numeric identifier of
#'   the job over the tasks.  These functions populate this value
#'   correctly and automatically; it is required in the database,
#'   but are never needed from the user when creating jobs.
#' - bettr_task_job_priority: Required.  A number indicating the
#'   job priority; the lower the number, the higher the priority.
#' - bettr_task_name: Required.  The name of the R task file to
#'   be run to fulfill the task.  Must be located here in the repo:
#'   <bettr_task_git_project>/src/R/tasks/<bettr_task_name>/<bettr_task_name>.R
#' - bettr_task_sort: Optional.  The order in which tasks must be
#'   completed within the job.  If not provided, defaults to the
#'   order that task rows were added.
#' - opt_start_dt: Optional.  Parametric "start datetime" for
#'   task R files.  This bounds the scope of ETL work to a specific
#'   range of time, subject to the task's interpretation of that.
#' - opt_end_dt:  Optional . See `opt_start_dt`.
#' - opt_cache_expiry_mins: Optional.  The cache expiry in minutes.
#'   Specifically, any task that completes can be give a value >= 1
#'   here to indicate that the task is only valid for that many
#'   minutes.  After the cache expires, the task is back in the list.
#'   Defaults to -1, indicating the task has no expiry time.
#' - opt_number_list: Optional.  Parametric "number list" for task
#'   R files.  Stored in the database as a comma-separated string,
#'   and is subject to that field's storage limits (4000 characters).
#' - opt_char_list: Optional.  See `opt_number_list`.
#' - last_task_started_dt: Metadata.  If this task was ever started,
#'   this field stores the datetime that happened.
#' - last_hb_dt: Metadata.  If this task was ever started, this field
#'   stores the last "heartbeat" datetime.  The heartbeat is sent
#'   to the host from the task process frequently while the task is
#'   running.
#' - last_status: Metadata.  If this task was ever started, this
#'   stores the last status code of the task, or 0 (Not Started)
#'   for tasks that were never run.
#'   The status codes are:
#'     - 0: Not Started
#'     - 10: Started
#'     - 20: Failed
#'     - 21: Failed, No Retry
#'     - 30: Succeeded
#' - last_error: Metadata.  If this task was ever started and it
#'   generated an error, this field contains a string summary of
#'   the error.  This field should be set each time the last_status
#'   is set.
#' - last_report: Metadata.  If this task was ever started,
#'   this stores a task-generated, JSON-formatted "report" object.
#'   The contents of this report are subjective, as tasks can run
#'   arbitrarily complex code, and is intended to provide values
#'   that can be supervised by an automated log interpreter.
#' @export
get_bettr_task_defaults <- function() {
  tibble::tibble(
    bettr_task_git_project = Sys.getenv("BETTR_TASK_GIT_PROJECT"),
    bettr_task_git_branch = Sys.getenv("BETTR_TASK_GIT_BRANCH"),
    bettr_task_git_commit = Sys.getenv("BETTR_TASK_GIT_COMMIT"),
    bettr_task_job_comment = as.character(NA),
    bettr_task_job_id = as.double(-1),
    bettr_task_job_priority = as.double(100),
    bettr_task_name = as.character("__BETTR_DEFAULTS__"),
    bettr_task_sort = as.double(-1),
    opt_start_dt = as.Date(NA),
    opt_end_dt = as.Date(NA),
    opt_cache_expiry_mins = as.double(-1),
    opt_number_list = as.character(NA),
    opt_char_list = as.character(NA),
    last_task_started_dt = as.Date(NA),
    last_hb_dt = as.Date(NA),
    last_status = as.double(0),
    last_error = as.character(NA),
    last_rows_affected = as.double(0),
    last_report = as.character("{}")
  )
}

#' Adds A Job to the Bettr Host Task Table
#' @param bettr_tasks The tasks to be added to the job
#' table.  Refer to `?bettr::get_bettr_task_defaults`
#' for information about bettr_task columns.
#' @export
add_job_to_host <- function(bettr_tasks) {
  tryCatch(
    {
      init_bettr_host()
      assert_valid_bettr_tasks(
        bettr_tasks = bettr_tasks,
        cols = c("bettr_task_name")
      )

      # Apply a default sort if necessary
      if (!c("bettr_task_sort") %in% names(bettr_tasks)) {
        bettr_tasks <- bettr_tasks |>
          dplyr::mutate(
            bettr_task_sort = dplyr::row_number()
          )
      }

      # Apply the next available job ID
      bettr_tasks <- bettr_tasks |>
        dplyr::mutate(
          bettr_task_job_id = get_rows(
            connection_name = "bettr_host",
            sql = "get_next_bettr_job_id"
          ) |>
            dplyr::pull() |>
            as.numeric()
        )

      # Apply simple defaults and send them to the host
      bettr_tasks |>
        apply_bettr_task_defaults() |>
        append_rows(
          connection_name = "bettr_host",
          table_name = "bettr_task"
        )
    },
    error = \(err) {
      stop(err)
    },
    warning = \(warn) {
      warning(warn)
    },
    finally = {}
  )
}

#' Run Next Project Job in Better Queue
#' 
#' This uses the provided git repository (project) and
#' branch name to query the host for the next runnable
#' job, being a whole group of tasks.
#' Note that this will attempt to rerun, in order, all
#' the tasks under the job, not just those expired,
#' errored, or not started.
#' This is the first job retrieved by this SQL:
#' `"{system.file(package = 'bettr')}/sql/BETTR_HOST/get_next_bettr_job.sql"`
#' 
#' @param project String name of the project, used to
#' filter for eligible jobs.
#' @param branch String name of the project branch, used
#' to filter for eligible jobs.
#' @param incl_live_refresh Boolean Indicates whether live
#' refresh tasks (with an opt_expiry_cache_mins > -1) should
#' be considered for the next job.  Defaults to TRUE.
#' @param incl_historical_refresh Boolean Indicates whether historical
#' refresh tasks (with an opt_expiry_cache_mins <= -1) should
#' be considered for the next job.  Defaults to TRUE.
#' @param return_result Boolean Indicates whether results
#' should be returned.  Result objects are useful for
#' debugging bug clutter up the job server log files.
#' Defaults to FALSE.
#' @param suppress_logging Boolean Suppresses the logging
#' of the subprocess standard out and error. Defaults to FALSE.
#' @export
run_next_job_in_queue <- function(
  project = Sys.getenv("BETTR_TASK_GIT_PROJECT"),
  branch = Sys.getenv("BETTR_TASK_GIT_BRANCH"),
  incl_live_refresh = TRUE,
  incl_historical_refresh = TRUE,
  return_result = FALSE,
  suppress_logging = FALSE) {
  next_bettr_job <- tibble::tibble(
    bettr_task_git_project = project,
    bettr_task_git_branch = branch,
    incl_live_refresh = dplyr::if_else(
      incl_live_refresh, 1, 0
    ),
    incl_historical_refresh = dplyr::if_else(
      incl_historical_refresh, 1, 0
    )
  ) |> get_rows(
    connection_name = "bettr_host",
    sql = "get_next_bettr_job"
  )

  if (next_bettr_job |> dplyr::count() == 0) {
    message("... No next job found, exiting")
    return()
  }

  task_results <- tibble::tibble()

  for (i in seq_len(nrow(next_bettr_job))) {
    message(
      stringr::str_glue("... starting job task {i}/{nrow(next_bettr_job)} ...")
    )
    # TODO: smarten this up and give it a spin
    # message(
    #   stringr::str_glue(paste(
    #     "... starting job task {i}/{nrow(next_bettr_job)},",
    #     "{next_bettr_job$bettr_task_name[1]}",
    #     "({next_bettr_job$bettr_task_job_comment[1]})..."
    #     )
    #   )
    # )
    task_result <- next_bettr_job |>
      dplyr::slice(i:i) |>
      dplyr::rename_all(snakecase::to_snake_case) |>
      run_task()

    error <- ""

    if (0 != length(task_result$rs_result$error)) {
      error <- stringr::str_glue(
        paste(
          "Error: {task_result$rs_result$error$message}",
          "{task_result$rs_result$error$parent$message}",
          sep = "\n"
        )
      ) |>
        as.character()
    }

    task_results <- task_results |>
      dplyr::bind_rows(
        task_result$bettr_task |>
          dplyr::mutate(
            run_code = task_result$rs_result$code,
            run_stdout = task_result$rs_result$stdout,
            run_stderr = task_result$rs_result$stderr,
            run_error = error
          )
      )

    if (!suppress_logging) {
      message(stringr::str_glue(
        "------ {as.character(task_result$bettr_task$bettr_task_name)} start"
      ))
      message("------ stdout")
      message(task_result$rs_result$stdout)
      message("------ stderr")
      message(task_result$rs_result$stderr)

      if (error != "") {
        message("!!!!!! error")
        message(error)
      }
      message(stringr::str_glue(
        "...... {as.character(task_result$bettr_task$bettr_task_name)} end"
      ))
    }
  }

  if (return_result) {
    task_results
  }
}

#' Run Next Project Task in Bettr Queue
#'
#' This uses the provided git repository (project) and
#' branch name to query the host for the next runnable
#' task.  This is the first row retrieved by this SQL:
#' `"{system.file(package = 'bettr')}/sql/BETTR_HOST/get_next_bettr_task.sql"`
#'
#' @param project String name of the project, used to
#' filter for eligible tasks.
#' @param branch String name of the project branch, used
#' to filter for eligible tasks.
#' @param incl_live_refresh Boolean Indicates whether live
#' refresh tasks (with an opt_expiry_cache_mins > -1) should
#' be considered for the next job.  Defaults to TRUE.
#' @param incl_historical_refresh Boolean Indicates whether historical
#' refresh tasks (with an opt_expiry_cache_mins <= -1) should
#' be considered for the next job.  Defaults to TRUE.
#' @param return_result Boolean Indicates whether results
#' should be returned.  Result objects are useful for
#' debugging bug clutter up the job server log files.
#' Defaults to FALSE.
#' @param suppress_logging Boolean Suppresses the logging
#' of the subprocess standard out and error. Defaults to FALSE.
#' @export
run_next_task_in_queue <- function(
  project = Sys.getenv("BETTR_TASK_GIT_PROJECT"),
  branch = Sys.getenv("BETTR_TASK_GIT_BRANCH"),
  incl_live_refresh = TRUE,
  incl_historical_refresh = TRUE,
  return_result = FALSE,
  suppress_logging = FALSE) {
  next_bettr_task <- tibble::tibble(
    bettr_task_git_project = project,
    bettr_task_git_branch = branch,
    incl_live_refresh = dplyr::if_else(
      incl_live_refresh, 1, 0
    ),
    incl_historical_refresh = dplyr::if_else(
      incl_historical_refresh, 1, 0
    )
  ) |> get_rows(
    connection_name = "bettr_host",
    sql = "get_next_bettr_task"
  )

  if (next_bettr_task |> dplyr::count() == 0) {
    message("... No next task found, exiting")
    return()
  }

  task_result <- next_bettr_task |>
    dplyr::slice_head(n = 1) |>
    dplyr::rename_all(snakecase::to_snake_case) |>
    run_task()

  if (!suppress_logging) {
    message(task_result$rs_result$stdout)
    message(task_result$rs_result$stderr)
  }

  if (return_result) {
    task_result
  }
}

#' Set Up Task Environment and Execute Task
#'
#' This function sets up an R session, environment
#' variables, and options to allow the R task file
#' to execute. This function also sets up heartbeat calls
#' to the bettr host.
#'
#' @param bettr_task Required.  A row from the BETTR_TASK table, or
#' a tibble created to look just like one.
#' @param hb_timeout Optional.  The time in milliseconds to wait
#' between calls to the heartbeat function.  Defaults to 10000L (10s).
run_task <- function(
  bettr_task,
  hb_timeout = 10000L
) {

  if (is.null(bettr_task) || bettr_task |> dplyr::count() != 1) {
    stop("!!! run_task() requires exactly 1 bettr_task")
  }

  rs_task <- init_task_session(bettr_task = bettr_task)
  rs_hb <- callr::r_session$new(wait = TRUE)

  message(
    stringr::str_glue(
      "... starting task {bettr_task$bettr_task_name[1]}"
    )
  )

  on.exit({
    message("... run_task(): closing R task and heartbeat sessions")
    rs_task$close()
    rs_hb$close()
  }, add = TRUE)

  tryCatch(
    {
      rs_task$call(
        func = \() {
          task_file <- Sys.getenv("TASK_FILE")
          tryCatch(
            {
              if (!file.exists(task_file)) {
                stop(
                  stringr::str_glue(
                    paste(
                      "!!! run_task() error in task: {task_file}.  ",
                      "Is the task R file in the correct folder?  ",
                      "<project_root>/src/R/tasks/<bettr_task_name>/<bettr_task_name>.R"
                    )
                  )
                )
              }
              source(task_file)
            },
            error = \(err) {
              stop(err)
            }
          )
        }
      )
      state <- "timeout"
      # Prepare the heartbeat session with the global vars that
      # do_heartbeat will need for execution
      rs_hb$run(func = \(bettr_task_key, last_task_started_dt, bettr_task_git_commit) {
          hb_args <<- list(
            bettr_task_key = bettr_task_key,
            last_task_started_dt = last_task_started_dt,
            bettr_task_git_commit = bettr_task_git_commit
          )
        },
        args = list(
          bettr_task_key = bettr_task$bettr_task_key,
          last_task_started_dt = lubridate::now(),
          bettr_task_git_commit = Sys.getenv("BETTR_TASK_GIT_COMMIT")
        )
      )
      while (state != "ready") {
        rs_hb$run(func = do_heartbeat, args = list())
        cat(".")
        state <- rs_task$poll_process(timeout = hb_timeout)
      }
      cat("\n")

      resolve_rs_task_result(
        rs_task = rs_task,
        rs_hb = rs_hb,
        bettr_task = bettr_task
      )
    },
    error = \(err) {
      message("!!! run_task error")
      stop(err)
    },
    warning = \(warn) {
      warning(warn)
    },
    finally = {
    }
  )
}

#' Send a Heartbeat to the Bettr Job Host
#'
#' This function updates BETTR_TASK.LAST_HB_DT in the bettr
#' host Oracle database.  Note: this function is relies on static
#' values in a global list object called `hb_args`, which must be set
#' before this function is called.
#' @param last_status Optional. Status code to send. Defaults to 10.
#' @param last_error Optional.  The string returned by the last error
#' returned by the task's `callr` R subprocess.  Defaults to empty.
do_heartbeat <- function(last_status = 10, last_error = "") {
  tibble::tibble(
    bettr_task_key = hb_args$bettr_task_key,
    last_task_started_dt = hb_args$last_task_started_dt,
    bettr_task_git_commit = hb_args$bettr_task_git_commit,
    last_status = last_status,
    last_error = last_error,
    last_hb_dt = lubridate::now()
  ) |>
    bettr::execute_stmts(
      connection_name = "bettr_host",
      sql = "update_bettr_task_last_run_details"
    )
}

#' Resolve An Ended Task Session with the Bettr Host
#'
#' @param rs_task Required.  A `callr::r_session` object where
#' our task R file has just stopped execution.
#' @param rs_hb Required.  A `callr::r_session` object for the
#' heartbeat.
#' @param bettr_task Required.  A bettr task object.  See the function
#' `get_bettr_task_defaults()` for details.
resolve_rs_task_result <- function(rs_task, rs_hb, bettr_task) {

  tryCatch(
    {
      result <- rs_task$read()
      last_status <- 30
      last_error <- ""

      if (!is.null(result$error)) {
        message("!!! Task generated an error; marking status as failed")
        last_status <- 20
        last_error <- result$error |>
          as.character() |>
          stringr::str_sub(start = 1L, end = 3999L)
      }

      rs_hb$run(func = do_heartbeat, args = list(
        last_status = last_status,
        last_error = last_error
      ))

      submit_task_report(
        bettr_task_key = bettr_task$bettr_task_key,
        last_report = result$result$value
      )

      list(
        rs_result = result,
        bettr_task = bettr_task |>
          dplyr::mutate(
            last_status = last_status,
            last_error = last_error
          )
      )
    },
    error = \(err) {
      stop(err)
    },
    warning = \(warn) {
      warning(warn)
    },
    finally = {}
  )
}

#' Creates and Centrally Logs A Task Report
#'
#' The field BETTR_TASK.LAST_REPORT stores the last "report"
#' generated by the task R file, if any.  This is simply
#' the last returned object from the task R file, converted
#' to JSON.
#' This function is provided as a convenience for client code
#' to explicitly self-report to the task table and convert
#' the data to JSON for their own needs.
#' @param last_report A dataframe, tibble, or list that can be
#' converted to JSON and sorted in the `last_report` field.
#' @return A string in JSON format of the given report. Note
#' that parsing with `jsonlite::toJSON` may fail with complex
#' objects and JSON strings over 4000 characters are dropped
#' for exceeding storage limits.
#' @export
submit_task_report <- function(bettr_task_key, last_report = NULL) {
  report_json <- jsonlite::toJSON(last_report)

  if (is.null(last_report)) {
    report_json <- "{}"
  }

  if (report_json |> length() > 4000) {
    report_json <- "{ json_error: \"!!! report exceeded 4000 characters\" }"
  }

  tibble::tibble(
    bettr_task_key = bettr_task_key,
    last_report = report_json
  ) |>
    bettr::execute_stmts(
      connection_name = "bettr_host",
      sql_file = "update_bettr_task_last_report"
    )

  report_json
}

#' Initialize the Bettr Host for Tasks
#'
#' Initializes the task table, if it is missing.
init_bettr_host <- function() {
  get_bettr_task_defaults() |>
    ensure_table(
      connection_name = "bettr_host",
      table_name = "bettr_task"
    )
}

#' Set Up An R Session to Run a Task
#'
#' This uses the library `callr` to create an R session
#' that loads the current environment variables, sets a
#' bettr_task object to the global `task_args`, and otherwise
#' sets up the session for a call to `bettr::run_task()`.
#' @param bettr_task Required.  A bettr_task object.
#' @return A `callr::r_session` object.
init_task_session <- function(bettr_task) {

  rs_task <- callr::r_session$new(wait = TRUE)

  tryCatch(
    {
      bettr_task <- bettr_task |>
        dplyr::slice_head(n = 1)

      bettr_task_name <- bettr_task$bettr_task_name[1] |>
        snakecase::to_snake_case()

      # I don't feel great about burying this constant,
      # but there really is no better place for it,
      # and it should not be configuration-driven
      base_path <- paste(
        "src",
        "R",
        "tasks",
        sep = .Platform$file.sep
      )

      task_dir <- paste(
        base_path,
        bettr_task_name,
        sep = .Platform$file.sep
      )

      task_file_path <- paste(
        task_dir,
        paste(
          bettr_task_name,
          "R",
          sep = "."
        ),
        sep = .Platform$file.sep
      )

      task_sql_dir <- paste(
        task_dir,
        "sql",
        sep = .Platform$file.sep
      )

      rs_task$run(
        \(task_dir, task_file_path, task_sql_dir, task_args) {
          Sys.setenv(
            TASK_DIR = task_dir,
            TASK_FILE = task_file_path,
            SQL_DIR = task_sql_dir
          )

          task_args <<- task_args
        },
        package = TRUE,
        args = list(
          task_dir = task_dir,
          task_file_path = task_file_path,
          task_sql_dir = task_sql_dir,
          task_args = bettr_task
        )
      )

      rs_task
    },
    error = \(err) {
      stop(err)
    },
    warning = \(warn) {
      warning(warn)
    },
    finally = {
    }
  )
}

#' Asserts a Bettr Task Has Needed Columns
#' @return The passed tasks to allow piping.
#' @export
assert_valid_bettr_tasks <- function(
  bettr_tasks = tibble::tibble(),
  cols = c()
) {
  if (dplyr::count(bettr_tasks) < 1L) {
    stop("No task provided")
  }

  task_cols <- names(bettr_tasks)
  invalid_cols_found <- task_cols[
    !task_cols %in% names(get_bettr_task_defaults())
  ]
  required_cols_missing <- cols[!cols %in% task_cols]

  error_message <- ""

  if (length(required_cols_missing) > 0) {
    error_message <- paste("\nMissing required columns: ",
      paste0(required_cols_missing, collapse = ","),
      sep = ""
    )
  }

  if (invalid_cols_found |> length() > 0) {
    error_message <- paste(
      error_message,
      "\nFound invalid columns: ",
      paste0(invalid_cols_found, collapse = ","),
      sep = ""
    )
  }

  if (error_message != "") {
    stop(
      paste("!!! assert_valid_bettr_tasks failed: \n",
        error_message,
        sep = ""
      )
    )
  }

  bettr_tasks
}

#' Asserts the Status of a Task on the Host
#'
#' Looks for exactly 1 task on the BETTR_HOST that
#' matches the criteria given by `bettr_task`.
#' This is intended to allow tasks to verify that
#' the most recent execution of different task matching
#' the attributes, one that provides data for the caller,
#' has already executed successfully on the host.
#' The logic determines "most recent execution" by sorting
#' on the LAST_STARTED_DT and taking the most recently
#' started task as the candidate.
#' Finding more than one candidate task is a sign of
#' duplicative ETL work, and while not an error, will
#' generate a warning that the task table needs review.
#' For more information on this argument, see the
#' definition for `bettr::get_bettr_tasks_by_attributes()`.
#' Be aware that `bettr_task_git_project`, `bettr_task_name`,
#' and `last_status` are required on `bettr_task`.
#' @param bettr_task Required.  A partial definition
#' of a `bettr_task` used as a filter for
#' finding exactly 1 matching task in BETTR_HOST.
#' @return TRUE if a single matching row is found;
#' throws an error in all other cases.
#' @export
assert_task_status <- function(bettr_task) {

  # Ensure all required fields on bettr_task are provided
  missing_cols <- setdiff(
    c("bettr_task_git_project", "bettr_task_name", "last_status"),
    colnames(bettr_task)
  )

  if (length(missing_cols) > 0) {
    stop(
      paste(
        "!!! bettr::assert_task_status failed; required columns missing: ",
        missing_cols,
        sep = "\n"
      )
    )
  }

  # Find all results regardless of last status
  task_results <- get_bettr_tasks_by_criteria(
    bettr_task |> dplyr::select(-c(last_status))
  )

  bettr_task_str <- bettr_task |>
    # Format dates to YYYY-MM-DD HH24:MI:SS strings
    dplyr::mutate(
      dplyr::across(tidyr::ends_with("_dt"), \(x) {
        dplyr::case_when(
          is.na(x) ~ as.character(NA),
          TRUE ~ format(x, "%Y-%m-%d %H:%M:%S")
        )
      })
    ) |>
    # Format all other values to strings
    dplyr::mutate(dplyr::across(dplyr::everything(), as.character))

  bettr_task_str <- paste0(
    stringr::str_glue(
      "{colnames(bettr_task)}: {bettr_task_str}"
    ),
    collapse = "\n"
  )

  if (task_results |> dplyr::count() |> as.double() == 0) {
    stop(
      paste(
        "!!! bettr::assert_task_status failed, no matches for criteria: ",
        bettr_task_str,
        sep = "\n"
      )
    )
  } else {
    task_latest_result <- task_results |>
      dplyr::arrange(dplyr::desc(last_task_started_dt)) |>
      dplyr::slice_head(n = 1)

    if (task_results |> dplyr::count() |> as.double() > 1L) {
      warning(
        "!!! bettr::assert_task_status found 2+ eligible tasks"
      )
    }

    task_latest_str <- task_latest_result |>
      # Format dates to YYYY-MM-DD HH24:MI:SS strings
      dplyr::mutate(
        dplyr::across(tidyr::ends_with("_dt"), \(x) {
          dplyr::case_when(
            is.na(x) ~ as.character(NA),
            TRUE ~ format(x, "%Y-%m-%d %H:%M:%S")
          )
        })
      ) |>
      # Format all other values to strings
      dplyr::mutate(dplyr::across(dplyr::everything(), as.character))

    task_latest_str <- paste0(
      stringr::str_glue(
        "{colnames(task_latest_result)}: {task_latest_str}"
      ),
      collapse = "\n"
    )

    if (task_latest_result$last_status[1] != bettr_task$last_status[1]) {
      stop(
        paste(
          "!!! bettr::assert_task_status failed, latest status mismatch",
          stringr::str_glue("(expected {bettr_task$last_status[1]}): "),
          task_latest_str,
          sep = "\n"
        )
      )
    }

    else {
      message(
        paste(
          "+++ bettr::assert_task_status succeeded: ",
          task_latest_str,
          sep = "\n"
        )
      )
    }
  }

  TRUE
}

#' Inspect Tasks on the Host
#'
#' DO NOT USE THIS FUNCTION UNLESS YOU ARE EXTREMELY
#' SURE YOU KNOW WHAT YOU ARE DOING.  It is possible
#' to lock the state of the BETTR_HOST, causing
#' all data extract jobs on the host to stop.  If you
#' don't know how to avoid that, avoid this function.
#'
#' Returns a list of tasks on the BETTR_HOST that
#' meet the criteria given by `bettr_tasks`.  This
#' can have multiple rows; each row queries the
#' BETTR_HOST separately.  Results of each execution
#' are returned in a union.
#' 
#' @param bettr_tasks Required.  A tibble with
#' one or more row of query criteria.  All values
#' are optional, but at least one must be provided.
#' Accepted filter columns and their match method:
#'
#' - `bettr_task_git_project`: Regex pattern.
#' - `bettr_task_job_comment`: Regex pattern.
#' - `bettr_task_name`: Regex pattern.
#' - `opt_start_dt`: LTE.
#' - `opt_end_dt`: GTE.
#' - `opt_cache_expiry_mins`: Negative/zero or positive.
#' - `opt_number_list`: Regex pattern.
#' - `opt_char_list`: Regex pattern.
#' - `last_status`: Regex pattern.
#' - `last_error`: Regex pattern.
#' @export
get_bettr_tasks_by_criteria <- function(bettr_tasks) {
  tryCatch(
    {
      # Declare default values for search
      bettr_task_args <- tibble::tibble(
        bettr_task_git_project = as.character(NA),
        bettr_task_job_comment = as.character(NA),
        bettr_task_name = as.character(NA),
        opt_start_dt = as.POSIXct(NA),
        opt_end_dt = as.POSIXct(NA),
        opt_cache_expiry_mins = as.character(NA),
        opt_number_list = as.character(NA),
        opt_char_list = as.character(NA),
        last_status = as.character(NA),
        last_error = as.character(NA)
      ) |>
        # Cross-join with input to fill missing values
        dplyr::select(
          -c(colnames(bettr_tasks))
        ) |>
        dplyr::cross_join(
          bettr_tasks
        ) |>
        # Format dates to YYYY-MM-DD HH24:MI:SS strings
        dplyr::mutate(
          opt_start_dt = dplyr::case_when(
            is.na(opt_start_dt) ~ as.character(NA),
            TRUE ~ format(opt_start_dt, "%Y-%m-%d %H:%M:%S")
          ),
          opt_end_dt = dplyr::case_when(
            is.na(opt_end_dt) ~ as.character(NA),
            TRUE ~ format(opt_end_dt, "%Y-%m-%d %H:%M:%S")
          )
        ) |>
        # Format all other values to strings
        dplyr::mutate(dplyr::across(dplyr::everything(), as.character)) |>
        # Append unit test check flag from environment
        dplyr::mutate(
          bettr_lib_test_mode = dplyr::if_else(
            Sys.getenv("__BETTR_LIB_TEST_MODE") == "Y",
            1, 0
          )
        )

      task_results <- bettr_task_args |>
        get_rows(
          connection_name = "BETTR_HOST",
          sql = "get_bettr_tasks_by_criteria"
        )

      # 2025-04-02
      # Some pretty-printing code if needed for debugging.
      # bettr_task_args_str <- paste0(
      #   stringr::str_glue(
      #     "{colnames(bettr_task_args)}: {bettr_task_args}"
      #   ),
      #   collapse = "\n"
      # )
      # rs_found_str <- stringr::str_glue(
      #   "... {dplyr::count(task_results)} results found"
      # )
      # task_results_str <- task_results |>
      #   dplyr::mutate(dplyr::across(dplyr::everything(), as.character))
      # task_results_str <- paste0(
      #     stringr::str_glue(
      #       "{colnames(task_results_str)}: {task_results_str}"
      #     ),
      #     collapse = "\n"
      #   )
      # message("... task_results_str: ")
      # message(task_results_str)

      task_results
    },
    error = \(err) {
      stop(err)
    },
    warning = \(warn) {
      warning(warn)
    },
    finally = {
    }
  )
}

#' Apply Default Values to Bettr Task
#'
#' Given one or more bettr_tasks, this function takes
#' default values from `get_bettr_task_defaults()` and
#' fills them in where NAs are stored and/or the column
#' is missing.
#' @return The input bettr_tasks with default values
#' for all unspecified columns or NA values.
#' @export
apply_bettr_task_defaults <- function(bettr_tasks) {
  assert_valid_bettr_tasks(bettr_tasks)
  defaults <- get_bettr_task_defaults()
  defaults |>
    dplyr::full_join(bettr_tasks, by = names(bettr_tasks)) |>
    dplyr::mutate(
      dplyr::across(
        dplyr::everything(),
        \(x) {
          dplyr::coalesce(
            # TODO Fix this...
            x,
            dplyr::pull(
              defaults |> dplyr::select(dplyr::cur_column())
            )
          )
        }
      ),
      .keep = "unused"
    ) |>
    dplyr::filter(
      bettr_task_name != "__BETTR_DEFAULTS__"
    )
}

