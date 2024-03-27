#' Run Next Task In Bettr Task Queue
#' 
#' Checks the next runnable task in the bettr task table
#' and runs it.  If no eligible task is found, this logic
#' will return with no side-effects.
#' NOTE: For complex reasons with our ROracle API implementation,
#' the order of these columns must match the order they were
#' in when the table was first created.

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
    last_rows_affected = as.double(0)
  )
}

# LAST_STATUS Codes
# 0 = Not Started
# 10 = Started
# 20 = Failed
# 21 = Failed, No Retry
# 30 = Succeeded

#' Adds A Job to the Bettr Host Task Table
#' @param bettr_tasks The tasks to be added to the job
#' table.  Refer to the bettr readme for information
#' about specifying bettr tasks for this function.
#' @export
add_job <- function(bettr_tasks) {
  tryCatch(
    {
      init_bettr_host()
      assert_valid_bettr_tasks(
        bettr_tasks = bettr_tasks,
        cols = c(
          "bettr_task_name"
        )
      )

      # Apply a default sort if necessary
      if (!c("bettr_task_sort") %in% names(bettr_tasks)) {
        bettr_tasks <- bettr_tasks %>%
          dplyr::mutate(
            bettr_task_sort = dplyr::row_number()
          )
      }

      # Apply the next available job ID
      bettr_tasks <- bettr_tasks %>%
        dplyr::mutate(
          bettr_task_job_id = bettr::get_rows(
            connection_name = "bettr_host",
            sql = "get_next_bettr_job_id"
          ) %>%
            dplyr::pull() %>%
            as.numeric()
        )

      # Apply current project and branch as task target if necessary
      if (!c("bettr_task_git_project") %in% names(bettr_tasks)) {
        bettr_tasks <- bettr_tasks %>%
          dplyr::mutate(
            bettr_task_git_project = Sys.getenv("BETTR_TASK_GIT_PROJECT"),
            bettr_task_git_branch = Sys.getenv("BETTR_TASK_GIT_BRANCH"),
            bettr_task_git_commit = Sys.getenv("BETTR_TASK_GIT_COMMIT")
          )
      }

      bettr_tasks %>%
        apply_bettr_task_defaults() %>%
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

#' Initialize the Bettr Host for Tasks
#' Initializes the task table, if it is missing.
#' @export
init_bettr_host <- function() {
  get_bettr_task_defaults() %>%
    ensure_table(
      connection_name = "bettr_host",
      table_name = "bettr_task"
    )
}

#' Run Next Project Task in Bettr Queue
#' 
#' @param project String name of the project, used to
#' filter for eligible tasks.
#' @export
run_next_task_in_queue <- function(project, branch) {
  # TODO check BETTR_TASK for the next eligible, runnable
  # task within the given project
  next_bettr_task <- tibble::tibble(
    bettr_task_git_project = project,
    bettr_task_git_branch = branch
  ) %>% get_rows(
    connection_name = "bettr_host",
    sql = "get_next_bettr_task"
  )

  # TODO if no eligible rows are returned, exit
  if (next_bettr_task %>% length() == 0) {
    message("... No next task found, exiting")
    return()
  }

  # TODO if a row is returned, set up a task environment
  # and run the task
  next_bettr_task %>%
    dplyr::slice_head(n = 1) %>%
    run_task()
}

#' Set Up Task Environment and Execute Task
#' 
#' This function sets up an R session, environment
#' variables, and options to allow the task function
#' to execute. This function also sets up heartbeat calls
#' to the bettr host.
#' 
#' @param bettr_task A row from the BETTR_TASK table
#' in the bettr host Oracle database.
#' @param task A function to call with the prepared
#' options and values stored in `bettr_task`.
run_task <- function(
  bettr_task = tibble::tibble(),
  hb_timeout = 1000L
) {

  bettr_task <- bettr_task %>%
    dplyr::slice_head(n = 1)

  bettr_task_key <- bettr_task$bettr_task_key[1]

  rs_task <- init_task_session(bettr_task = bettr_task)
  rs_hb <- callr::r_session$new(wait = TRUE)

  on.exit({
    message("... closing R task and heartbeat sessions")
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
              source(task_file)
            },
            error = \(err) {
              cat(stringr::str_glue("!!! task_file: {task_file}"))
              stop(err)
            }
          )
        }
      )
      state <- "timeout"
      while (state != "ready") {
        rs_hb$run(func = do_heartbeat, args = list(
          bettr_task_key = bettr_task_key
        ))
        cat(".")
        state <- rs_task$poll_process(timeout = hb_timeout)
      }
      cat("\n")

      # TODO resolve the result of the task based on output.
      # For example, update status in BETTR_TASK.
      print(rs_task$read())

      message("... run_task complete")
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

#' Set Up Task Environment
#'
#' This function loads environment variables and prepares
#' an options object for use by a task function.
#'
init_task_session <- function(bettr_task) {

  rs_task <- callr::r_session$new(wait = TRUE)

  tryCatch(
    {
      bettr_task <- bettr_task %>%
        dplyr::slice_head(n = 1)

      bettr_task_name <- bettr_task$bettr_task_name[1] %>%
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

      message(stringr::str_glue("... task_dir: {task_dir}"))
      message(stringr::str_glue("... task_file_path: {task_file_path}"))
      message(stringr::str_glue("... task_sql_dir: {task_sql_dir}"))

      rs_task$run(
        \(task_dir, task_file_path, task_sql_dir, task_args) {
          # TODO try importing magrittr pipe here...
          # That would allow task functions to use it,
          # and not depend on the R >= 4.1 native pipe, |>
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

  if (invalid_cols_found %>% length() > 0) {
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

#' Send a Heartbeat to the Bettr Job Host
#' 
#' This function updates BETTR_TASK.LAST_HB_DT in the bettr
#' host Oracle database.
#' @param bettr_task_key Primary key of the row to update.
do_heartbeat <- function(bettr_task_key) {
  tibble::tibble(
    bettr_task_key = bettr_task_key,
    last_hb_dt = lubridate::now()
  ) |>
    bettr::execute_stmts(
      connection_name = "bettr_host",
      sql = "update_bettr_task_last_hb_dt"
    )
}

#' Apply Default Values to Bettr Task
apply_bettr_task_defaults <- function(bettr_tasks) {
  assert_valid_bettr_tasks(bettr_tasks)
  defaults <- get_bettr_task_defaults()
  defaults %>%
    dplyr::full_join(bettr_tasks, by = names(bettr_tasks)) %>%
    dplyr::mutate(
      dplyr::across(
        dplyr::everything(),
        ~dplyr::coalesce(
          .,
          dplyr::pull(
            defaults %>% dplyr::select(dplyr::cur_column())
          )
        )
      ),
      .keep = "unused"
    ) %>%
    dplyr::filter(
      bettr_task_name != "__BETTR_DEFAULTS__"
    )
}