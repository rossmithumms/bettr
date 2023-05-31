# Use this file to write up all your R functions to simplify data access.
#' @importFrom rlang .data

get_sql_dir <- function() {
  sql_dir <- Sys.getenv("SQL_DIR")
  
  if (is.na(sql_dir) | sql_dir == "") {
    stop("!!! \"sql_dir\" must be defined for functions.R")
  }
}

# https://www.r-bloggers.com/2019/08/no-visible-binding-for-global-variable/
globalVariables(c("session", "parseQueryString"))

#' Open a Database Connection
#' 
#' This function opens a database connection.
#' It depends on environment variables following this convention:
#' <CONNECTION_NAME>_CREDS_USR: database user name
#' <CONNECTION_NAME>_CREDS_PSW: database user password
#' <CONNECTION_NAME>_DB_NAME: database connection string (tnsnames.ora name OR Oracle connection string)
#' 
#' @param connection_name Database connection name; prefix to environment variables.
#' @return A DBI connection object
open_db_conn <- function(connection_name = "micpr") {
  username <- Sys.getenv(paste(toupper(connection_name), "CREDS", "USR", sep = "_"))
  password <- Sys.getenv(paste(toupper(connection_name), "CREDS", "PSW", sep = "_"))
  dbname <- Sys.getenv(paste(toupper(connection_name), "DB", "NAME", sep = "_"))

  RORacle::dbConnect(drv = RORacle::dbDriver("Oracle"),
    username = username, password = password, dbname = dbname
  )
}

#' Close a Database Connection
#' 
#' This function closes a database connection.
#' 
#' @param conn The connection object to close.
#' @return Response from DBI::dbDisconnect
close_db_conn <- function(conn) {
  ROracle::dbDisconnect(conn)
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
#' @param args a data frame, data.table, or tibble.  Query will be called once for each row.
#' @param connection_name Name of the database connection.  This is used to retrieve the
#' environment variables to open the connection and locate the SQL query in the SQL_DIR directory.
#' @param sql Name of the SQL file to be used for the query.
#' @return A tibble of the result set with cleaned column names.
#' @export
get_rows <- function(args, connection_name, sql) {
  tryCatch({
      connection_name <- toupper(connection_name)
      conn <- open_db_conn(connection_name = connection_name)
      sql <- load_sql(sql = sql, connection_name = connection_name)

      args %>%
        dplyr::select(get_bind_colnames(sql)) %>%
        as.list %>%
        purrr::list_transpose %>%
        purrr::map(~{
          message("+++ binding...")
          rs <- ROracle::dbSendQuery(conn, sql, bind_rows(.))
          message("+++ fetching...")
          data <- ROracle::fetch(rs)
          ROracle::dbClearResult(rs)
          return(data)
        }) %>%
          data.table::rbindlist %>%
          tibble::tibble %>%
          janitor::clean_names
    },

    warning = function(warn) {
      warning(warn)
    },

    error = function(err) {
      message(RORacle::dbGetException(conn))
      stop(err)
    },

    finally = {
      message("+++ closing connection")
      close_db_conn(conn)
    }
  )
}

append_to_table <- function(rows, conn, name, schema) {
  # Check to see if the table exists;
  # If not, create it with rows' cleaned column names and types
  # If so, list the fields and select the matches in the row data
    # Use symdiff(x, y) to find missing columns and generate warnings
  # Compare the data type from the server to the rows and coerce
    # Generate coercion warnings
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

load_sql <- function(sql, connection_name = NA) {
  if (!is.na(stringr::str_match(sql, "[-_a-zA-Z0-9]+"))) {
    sql_path <- here::here(
      get_sql_dir(), toupper(connection_name), paste(sql,  "sql", sep = ".")
    ) #nolint

    if (file.exists(sql_path)) {
      readr::read_file(file = sql_path)
    } else {
      stop(stringr::str_glue("!!! SQL file not found: {sql_path}"))
    }
  }
}

get_bind_colnames <- function(sql) {
  sql %>%
    stringr::str_match_all("&([a-z0-9_]+)") %>%
    data.frame %>%
    dplyr::pull(.data$X2)
}