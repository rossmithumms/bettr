#' Run Next Task In Bettr Task Queue
#' 
#' Checks the next runnable task in the bettr task table
#' and runs it.  If no eligible task is found, this logic
#' will return with no side-effects.

#' Initialize the Bettr Host for Tasks
#' 
#' Initializes the task table, if it is missing.
#' @export
init_bettr_host <- function() {
  # TODO check to see if the bettr task table exists;
  # if not, create it
}

#' Run Next Project Task in Bettr Queue
#' 
#' @param project String name of the project, used to
#' filter for eligible tasks.
#' @export
run_next_task_in_queue <- function(project) {
  # TODO check BETTR_TASK for the next eligible, runnable
  # task within the given project

  # TODO if no eligible rows are returned, exit

  # TODO if a row is returned, set up a task environment
  # and run the task

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

  rs_task <- init_task_session(
    bettr_task = bettr_task,
    # I don't feel great about burying this constant,
    # but there really is no better place for it,
    # and it should not be configuration-driven
    tasks_base_dir = paste(
      "src",
      "R",
      "tasks",
      sep = .Platform$file.sep
    )
  )
  rs_hb <- callr::r_session$new(wait = TRUE)
  on.exit({
    rs_task$close()
    rs_hb$close()
  }, add = TRUE)

  tryCatch(
    {
      rs_task$call(
        func = \() {
          source(Sys.getenv("TASK_FILE"))
        }
      )
      state <- "timeout"
      while (state != "ready") {
        rs_hb$run(func = do_heartbeat)
        cat(".")
        state <- rs_task$poll_process(timeout = hb_timeout)
      }
      cat("\n")

      # TODO resolve the result of the task based on output.
      # For example, update status in BETTR_TASK.
      print(rs_task$read())
    },
    error = \(err) {
      stop(err)
    },
    warning = \(warn) {
      warning(warn)
    },
    finally = {
      message("... run_task complete")
    }
  )
}

#' Set Up Task Environment
#'
#' This function loads environment variables and prepares
#' an options object for use by a task function.
#'
init_task_session <- function(
  bettr_task = tibble::tibble(),
  base_path
) {

  assert_valid_bettr_task(
    bettr_task,
    c("bettr_task_name")
  )

  rs_task <- callr::r_session$new(wait = TRUE)

  tryCatch(
    {
      task_dir <- paste(
        base_path,
        snakecase::to_snake_case(bettr_task$bettr_task_name[1]),
        sep = .Platform$file.sep
      )

      task_file_path <- paste(
        task_dir,
        paste(
          snakecase::to_snake_case(bettr_task$bettr_task_name[1]),
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

      message(stringr::str_paste("... task_dir: {task_dir}"))
      message(stringr::str_paste("... task_file_path: {task_file_path}"))
      message(stringr::str_paste("... task_sql_dir: {task_sql_dir}"))

      rs_task::run(
        \(task_dir, task_file_path, task_sql_dir, task_args) {
          readRenviron(".Renviron")
          Sys.setenv(
            TASK_DIR = task_dir,
            TASK_FILE = task_file_path,
            SQL_DIR = task_sql_dir
          )
          task_args <<- task_args
        },
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
      message("... init_task_session complete")
    }
  )
}

#' Asserts a Bettr Task Has Needed Columns
#' @export
assert_valid_bettr_task <- function(
  bettr_task = tibble::tibble(),
  cols = c()
) {
  if (dplyr::count(bettr_task) != 1L) {
    stop("No task provided")
  }

  missing_cols <- setdiff(names(bettr_task), cols)

  if (length(missing_cols) > 0) {
    stop(
      paste(
        "!!! validate_bettr_task failed, missing columns: ",
        paste0(missing_cols, collapse = ","),
        sep = ""
      )
    )
  }
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
