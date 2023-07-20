# Use this file to write up all your R functions to simplify data access.
#' @importFrom rlang .data

# https://www.r-bloggers.com/2019/08/no-visible-binding-for-global-variable/
globalVariables(c("session", "parseQueryString"))

#' Open a Database Connection
#'
#' This function opens a database connection.
#' It depends on environment variables following this convention:
#' <CONNECTION_NAME>_CREDS_USR: database user name
#' <CONNECTION_NAME>_CREDS_PSW: database user password
#' <CONNECTION_NAME>_DB_NAME: database connection string
#' (tnsnames.ora name OR Oracle connection string)
#'
#' @param connection_name Database connection name; prefix to env vars.
#' @return A DBI connection object
#' @export
open_db_conn <- function(connection_name = "micpr") {
  ROracle::dbConnect(drv = DBI::dbDriver("Oracle"),
    username = get_db_username(connection_name),
    password = get_db_password(connection_name),
    dbname = get_db_name(connection_name)
  )
}

#' Get the Database Username
#'
#' @param connection_name the snake_case name of the connection.
#' @return The database username for that connection as defined in .Renviron
#' @export
get_db_username <- function(connection_name) {
  Sys.getenv(paste(toupper(connection_name), "CREDS", "USR", sep = "_"))
}

#' Get the Database Password
#'
#' @param connection_name the snake_case name of the connection.
#' @return The database password for that connection as defined in .Renviron
#' @export
get_db_password <- function(connection_name) {
  Sys.getenv(paste(toupper(connection_name), "CREDS", "PSW", sep = "_"))
}

#' Get the Database Connection String or TNS Name
#'
#' @param connection_name the snake_case name of the connection.
#' @return The database connection String as defined in .Renviron
#' @export
get_db_name <- function(connection_name) {
  Sys.getenv(paste(toupper(connection_name), "DB", "NAME", sep = "_"))
}

#' Close a Database Connection
#'
#' This function closes a database connection.
#'
#' @param conn The connection object to close.
#' @return Response from DBI::dbDisconnect
#' @export
close_db_conn <- function(conn) {
  ROracle::dbDisconnect(conn)
  message("... disconnected")
}

#' Get Rows from a Database
#' 
#' This function allows for parameterized, batch SELECT queries to an Oracle connection.
#' The SQL statement must be stored in a file located in the following path location:
#' <SQL_DIR>/<CONNECTION_NAME><SQL>.sql
#' 
#' The query will be called for each row in the args object (a data frame, data.table, 
#' or tibble).  The SQL file may expose bind parameters that will be used through the
#' DBI bind API.  The named bind parameters must be unique, begin with a question mark,
#' and be formatted in snake_case.
#' 
#' The SQL_DIR must be provided as an absolute path in an environment variable.
#' 
#' By requiring the SQL to be stored outside this file it can be versioned, tested, and
#' audited separately from the R script calling this function.
#' 
#' @param binds a data frame, data.table, or tibble with bind parameters.  Query will be called once for each row.
#' @param connection_name The snake_case name of the connection. This is used to retrieve the
#' environment variables to open the connection and locate the SQL query in the SQL_DIR directory.
#' @param sql Name of the SQL file to be used for the query.
#' @param suppress_bind_logging Optionally suppress the logging of bind parameters (defaults to false).
#' @return A tibble of the result set with cleaned column names.
#' @export
get_rows <- function(binds, connection_name, sql, suppress_bind_logging = FALSE) {
  tryCatch({
      connection_name <- toupper(connection_name)
      conn <- open_db_conn(connection_name = connection_name)
      sql_statement <- load_sql(sql = sql, connection_name = connection_name)

      output <- binds %>%
        dplyr::rename_with(toupper) %>%
        dplyr::select(get_bind_colnames(sql_statement)) %>%
        as.list() %>%
        purrr::list_transpose() %>%
        purrr::map(~{
          message(stringr::str_glue("... querying {connection_name}: {sql}"))
          if (suppress_bind_logging == FALSE) {
            message(paste0(c(
                "... binding: ",
                stringr::str_glue("{names(.)} = {paste(.)}; ")
            )))
          } else {
            message("... binding: <suppressed>")
          }
          tictoc::tic()
          rs <- ROracle::dbSendQuery(conn, sql_statement, dplyr::bind_rows(.))
          tictoc::toc()
          message("... fetching")
          tictoc::tic()
          data <- ROracle::fetch(rs)
          tictoc::toc()
          ROracle::dbClearResult(rs)
          message(stringr::str_glue("... returning {dplyr::count(data)} rows"))
          data
        }) %>%
          data.table::rbindlist() %>%
          tibble::tibble() %>%
          janitor::clean_names()

      message(".... THIS should actually result in a 16,500-ish number:")
      message(dplyr::count(output))
      return(output)
    },

    warning = function(warn) {
      warning(warn)
    },

    error = function(err) {
      message(ROracle::dbGetException(conn))
      stop(err)
    },

    finally = {
      message("+++ closing connection")
      close_db_conn(conn)
    }
  )
}

#' Append Rows to a Table
#'
#' Take the passed rows and append them to a table given a connection.
#' If the table does not exist, it is created with `ensure_table()`.
#' Note: the schema for created tables is expected to be the database username.
#'
#' @param rows a data frame, data.table, or tibble with rows of values to insert
#' @param connection_name The snake_case name of the connection.
#' @param table_name The name of the table to receive data.
#' @param suppress_bind_logging Optionally suppress the logging of bind parameters (defaults to false).
#' @export
append_rows <- function(rows, connection_name, table_name, suppress_bind_logging = FALSE) {
  tryCatch({
      connection_name <- toupper(connection_name)
      schema <- get_db_username(connection_name)
      table_name <- toupper(table_name)

      # If the table does not exist yet, create it
      ensure_table(connection_name = connection_name, table_name = table_name)

      # Then write rows into it
      conn <- open_db_conn(connection_name = connection_name)
      sql_statement <- paste0(
        "INSERT INTO ",
        get_db_username(connection_name), ".",
        table_name, " (",
        paste(
          rows %>%
            names() %>%
            toupper(),
          collapse = ", "
        ),
        ") VALUES (&",
        paste(
          rows %>%
            names() %>%
            toupper(),
          collapse = ", &"
        ),
        ")"
      )
      # TODO filter rows down to columns in table

      # TODO write rows one at a time to database
      rows %>%
        dplyr::rename_with(toupper) %>%
        as.list() %>%
        purrr::list_transpose() %>%
        purrr::walk(~{
          if (suppress_bind_logging == FALSE) {
            message(stringr::str_glue("... querying {connection_name}: {sql_statement}"))
            message(paste0(c("... binding: ", stringr::str_glue("{names(.)} = {paste(.)}; "))))
          } else {
            message("... binding: <suppressed>")
          }
          tictoc::tic()
          result <- ROracle::dbSendQuery(conn, sql_statement, dplyr::bind_rows(.))
          success <- ROracle::dbGetInfo(result)$completed
          message(stringr::str_glue("... success: {success}"))
          if (success == TRUE) {
            ROracle::dbCommit(conn = conn)
          }
          ROracle::dbClearResult(result)
          tictoc::toc()
        })
    },

    warning = function(warn) {
      warning(warn)
    },

    error = function(err) {
      message(ROracle::dbGetException(conn))
      stop(err)
    },

    finally = {
      close_db_conn(conn)
    }
  )
}

#' Ensure a Table Exists for Appending Rows
#' 
#' This function ensures a table exists in the named connection for appending rows.
#' Note that this function depends on a SQL file named `create_table_<table_name>.sql`
#' in the connection-specific directory.
#' 
#' @param connection_name The snake_case name of the connection.
#' @param table_name The snake_case name of the table to ensure exists in the database.
#' @return Nothing
#' @export
ensure_table <- function(connection_name, table_name) {
  tryCatch({
      connection_name <- toupper(connection_name)
      conn <- open_db_conn(connection_name = connection_name)
      table_name <- toupper(table_name)

      # If the table does not exist yet, create it
      table_exists <- exists_table(
        connection_name = connection_name,
        table_name = table_name
      )

      message(stringr::str_glue("... table exists: {table_exists}"))

      if (table_exists == FALSE) {
        result <- ROracle::dbSendQuery(  # execute(
          conn = conn,
          statement = load_sql(
            tolower(stringr::str_glue("create_table_{table_name}")),
            connection_name = connection_name
          )
        )

        success <- ROracle::dbGetInfo(result)$completed
        if (success) {
          ROracle::dbCommit(conn = conn)
        }
        ROracle::dbClearResult(result)
        message(stringr::str_glue("+++ created table {table_name}, result: {success}"))
      } else {
        message(stringr::str_glue("+++ table already exists: {table_name}"))
      }

      TRUE
    },

    warning = function(warn) {
      warning(warn)
    },

    error = function(err) {
      message(ROracle::dbGetException(conn))
      stop(err)
    },

    finally = {
      close_db_conn(conn)
    }
  )
}

#' Check if a Table Exists
#' 
#' Implementing this function here because ROracle::dbExistsTable is being weird.
#' 
#' @param connection_name The snake_case name of the connection.
#' @param table_name The snake_case name of the table to drop.
#' @export
exists_table <- function(connection_name, table_name) {
  tryCatch({
      connection_name <- toupper(connection_name)
      conn <- open_db_conn(connection_name = connection_name)
      table_name <- toupper(table_name)
      bad_characters <- stringr::str_detect(table_name, "[^_A-Za-z0-9]")

      if (bad_characters == TRUE) {
        stop("!!! Invalid characters detected in table name; stopping")
      }

      # If the table does not exist yet, create it
      rs <- ROracle::dbSendQuery(  # execute(
        conn = conn,
        statement = stringr::str_glue(
          "SELECT TABLE_NAME FROM USER_TABLES WHERE UPPER(TABLE_NAME) = &table_name"
        ),
        data.frame(table_name = table_name)
      )
      result <- ROracle::fetch(rs)
      ROracle::dbClearResult(rs)
      0 < dplyr::count(result)
    },

    warning = function(warn) {
      warning(warn)
    },

    error = function(err) {
      message(ROracle::dbGetException(conn))
      stop(err)
    },

    finally = {
      close_db_conn(conn)
    }
  )
}

#' Drop a Table
#' 
#' Drops a table from the database.
#' Note that this function depends on a SQL file named `drop_table_<table_name>.sql`
#' in the connection-specific directory.
#' 
#' @param connection_name The snake_case name of the connection.
#' @param table_name The snake_case name of the table to drop.
#' @export
drop_table <- function(connection_name, table_name) {
  tryCatch({
      connection_name <- toupper(connection_name)
      conn <- open_db_conn(connection_name = connection_name)
      schema <- get_db_username(connection_name)

      # If the table does not exist yet, create it
      table_exists <- exists_table(
        connection_name = connection_name,
        table_name = table_name
      )

      if (table_exists == TRUE) {
        result <- ROracle::dbSendQuery(
          conn = conn,
          statement = load_sql(
            tolower(stringr::str_glue("drop_table_{table_name}")),
            connection_name = connection_name
          )
        )
        success <- ROracle::dbGetInfo(result)$completed
        if (success) {
          ROracle::dbCommit(conn = conn)
        }
        ROracle::dbClearResult(result)
        message(stringr::str_glue("+++ drop table {table_name}, result: {success}"))
        return(success)
      } else {
        message(stringr::str_glue("+++ table does not exist: {table_name}"))
        return(FALSE)
      }
    },

    warning = function(warn) {
      warning(warn)
    },

    error = function(err) {
      message(ROracle::dbGetException(conn))
      stop(err)
    },

    finally = {
      close_db_conn(conn)
    }
  )
}

#' Get URL Query Parameters
#' 
#' What it says on the tin.  If the session contains a URL query,
#' this sugar method retrieves it for you.  You can also test by passing
#' a url_query.
#' 
#' @param url_query A URL query string to parse (optional).  If not passed, session URL query is used.
#' @return Named list of values from URL query
#' @export
get_url_query <- function(url_query = "") {
  tryCatch({
      if ("" == url_query & exists("session")) {
      url_query <- session$clientData$url_search # nolint
    }

    url_query %>% parseQueryString # nolint
  },

  warning = function(warn) {
    warning(warn)
  },

  error = function(err) {
    stop(err)
  })
}

#' Get the Directory where SQL Queries are Stored
#' 
#' All the functions in this module require that SQL statements are stored in external files.
#' This function refers to the environment variable SQL_DIR and retrieves it for use.
#' 
#' @return The path to the directory where SQL queries are stored.
#' @export
get_sql_dir <- function() {
  sql_dir <- Sys.getenv("SQL_DIR")
  
  if (is.na(sql_dir) | sql_dir == "") {
    stop("!!! \"sql_dir\" must be defined for bettr to function")
  }

  sql_dir
}

#' Load a SQL Statement from an External File
#' 
#' All the functions in this module require that SQL statements are stored in external files.
#' This function loads a SQL statement from a file.
#' 
#' @param sql The snake_case name of the SQL file without the file extension (such as `get_test_data`).
#' @param connection_name The snake_case name of the database connection.
#' @return The SQL statement stored in the file.
#' @export
load_sql <- function(sql, connection_name = NA) {
  if (!is.na(stringr::str_match(sql, "[^-_a-zA-Z0-9]"))) {
    stop("!!! SQL file name contains illegal characters; stopping")
  }

  sql_path <- here::here(
    get_sql_dir(), toupper(connection_name), paste(sql,  "sql", sep = ".")
  ) #nolint

  if (file.exists(sql_path)) {
    readr::read_file(file = sql_path)
  } else {
    stop(stringr::str_glue("!!! SQL file not found: {sql_path}"))
  }
}

#' Get the Names of Column Binds in a SQL Statement
#' 
#' The query parameters to retrieve data from a database are handled in RORacle as bind parameters.
#' This function scans a SQL statement for ampersand-prefixed, snake_cased bind parameters.
#' 
#' @param sql The SQL statement to scan for bind parameters.
#' @return A character vector of snake_cased bind parameter names to be used in dplyr verbs.
#' @export
get_bind_colnames <- function(sql) {
  sql %>%
    stringr::str_match_all("&([a-zA-Z0-9_]+)") %>%
    data.frame %>%
    dplyr::pull(.data$X2) %>%
    toupper
}
