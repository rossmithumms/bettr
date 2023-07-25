# Use this file to write up all your R functions to simplify data access.
#' @importFrom rlang .data

# https://www.r-bloggers.com/2019/08/no-visible-binding-for-global-variable/
globalVariables(c("session", "parseQueryString"))

#' Get a Database Connection
#' 
#' This function returns a database connection from a simple pool.
#' We expect each R script invocation to use 1 connection at most per data source.
#' 
#' @param connection_name Database connection name; prefix to env vars.
#' @return A DBI connection object
#' @export
get_db_conn <- function(connection_name) {
  connection_name <- toupper(connection_name)
  if (exists("bettr_connection_pool") == FALSE) {
    message("+++ creating new connection pool")
    bettr_connection_pool <<- list()
  }

  if (connection_name %in% names(bettr_connection_pool) == FALSE) {
    bettr_connection_pool[[connection_name]] <<- open_db_conn(
      connection_name = connection_name
    )
    message(stringr::str_glue("+++ opened pool connection: {connection_name}"))
  }

  bettr_connection_pool[[connection_name]]
}

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
open_db_conn <- function(connection_name) {
  connection_name <- toupper(connection_name)
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
get_db_username <- function(connection_name) {
  Sys.getenv(paste(toupper(connection_name), "CREDS", "USR", sep = "_"))
}

#' Get the Database Password
#'
#' @param connection_name the snake_case name of the connection.
#' @return The database password for that connection as defined in .Renviron
get_db_password <- function(connection_name) {
  Sys.getenv(paste(toupper(connection_name), "CREDS", "PSW", sep = "_"))
}

#' Get the Database Connection String or TNS Name
#'
#' @param connection_name the snake_case name of the connection.
#' @return The database connection String as defined in .Renviron
get_db_name <- function(connection_name) {
  Sys.getenv(paste(toupper(connection_name), "DB", "NAME", sep = "_"))
}

#' Close Database Connection Pool
#'
#' This function closes all database connections in the pool.
#' @export
close_db_conn_pool <- function() {
  names(bettr_connection_pool) %>%
    as.list() %>%
    lapply(function(connection_name) {
      message(stringr::str_glue("... closing connection {connection_name}"))
      ROracle::dbDisconnect(bettr_connection_pool[[connection_name]])
    })

  bettr_connection_pool <- list()
  message("!!! pool closed")
}

#' Get Rows from a Database
#' 
#' This function allows for parameterized, batch SELECT queries to an Oracle connection.
#' The SQL statement must be stored in a file located in the following path location:
#' <SQL_DIR>/<CONNECTION_NAME>/<SQL>.sql
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
      conn <- get_db_conn(connection_name = connection_name)
      sql_statement <- load_sql(sql = sql, connection_name = connection_name)

      binds %>%
        dplyr::rename_with(toupper) %>%
        dplyr::select(get_bind_colnames(sql_statement)) %>%
        as.list() %>%
        purrr::list_transpose() %>%
        lapply(function(row_binds) {
          message(stringr::str_glue("... querying {connection_name}: {sql}"))
          if (suppress_bind_logging == FALSE) {
            message(paste0(c(
                "... binding: ",
                stringr::str_glue("{names(row_binds)} = {paste(row_binds)}; ")
            )))
          } else {
            message("... binding: <suppressed>")
          }
          rs <- ROracle::dbSendQuery(conn, sql_statement, dplyr::bind_rows(row_binds))
          message("... fetching")
          data <- ROracle::fetch(rs)
          ROracle::dbClearResult(rs)
          message(stringr::str_glue("... returning {dplyr::count(data)} rows"))
          data
        }) %>%
          data.table::rbindlist() %>%
          tibble::as_tibble(.name_repair = snakecase::to_snake_case)
    },

    warning = function(warn) {
      warning(warn)
    },

    error = function(err) {
      message(ROracle::dbGetException(conn))
      stop(err)
    },

    finally = {
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

      # If the table does not exist yet, create and initialize it
      ensure_table(
        rows = rows,
        connection_name = connection_name,
        table_name = table_name
      )

      # Append to the table, assuming it exists already
      conn <- get_db_conn(connection_name = connection_name)
      ROracle::dbWriteTable(
        conn = conn,
        name = table_name,
        value = rows %>% dplyr::mutate(key = as.numeric(NA), audit_insert_dt = as.Date(NA)),
        schema = schema,
        row.names = FALSE,
        append = TRUE
      )
    },

    warning = function(warn) {
      warning(warn)
    },

    error = function(err) {
      message(ROracle::dbGetException(conn))
      stop(err)
    },

    finally = {
    }
  )
}

#' Ensure a Table Exists for Appending Rows
#' 
#' This function ensures a table exists in the named connection for appending rows.
#' Note that this function depends on a SQL file named `create_table_<table_name>.sql`
#' in the connection-specific directory.
#' 
#' @param rows Table structure with representative data for the table to create/verify.
#' @param connection_name The snake_case name of the connection.
#' @param table_name The snake_case name of the table to ensure exists in the database.
#' @return Nothing
#' @export
ensure_table <- function(rows, connection_name, table_name) {
  tryCatch({
      connection_name <- toupper(connection_name)
      conn <- get_db_conn(connection_name = connection_name)
      table_name <- toupper(table_name)
      schema <- get_db_username(connection_name)

      # If the table does not exist yet, create it
      table_exists <- exists_table(
        connection_name = connection_name,
        table_name = table_name
      )

      message(stringr::str_glue("... table exists: {table_exists}"))

      if (table_exists == FALSE) {
        # Create the table with the dbWriteTable API with one row,
        # then delete the row.  This is the simplest way to ensure typing.
        ROracle::dbWriteTable(
          conn = conn,
          name = table_name,
          value = rows %>% dplyr::slice_head(n = 1),
          schema = schema,
          row.names = FALSE,
          override = FALSE,
          append = FALSE
        )

        ROracle::dbSendQuery(
          conn = conn,
          statement = stringr::str_glue(
            stringr::str_glue("DELETE FROM {table_name}")
          ),
          data.frame(table_name = table_name)
        )

        # Now initialize the table by appending auto-incrementing
        # key columns, indexes, views, etc
        execute_stmts(
          connection_name = connection_name,
          sql = tolower(stringr::str_glue("init_{table_name}"))
        )

        ROracle::dbCommit(conn = conn)
        message(stringr::str_glue("+++ initialized table: {table_name}"))
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
    }
  )
}

#' Check if a Table Exists
#' 
#' Implementing this function here because ROracle::dbExistsTable is being weird.
#' 
#' @param connection_name The snake_case name of the connection.
#' @param table_name The snake_case name of the table to drop.
exists_table <- function(connection_name, table_name) {
  tryCatch({
      connection_name <- toupper(connection_name)
      conn <- get_db_conn(connection_name = connection_name)
      table_name <- toupper(table_name)
      bad_characters <- stringr::str_detect(table_name, "[^_A-Za-z0-9]")

      if (bad_characters == TRUE) {
        stop("!!! Invalid characters detected in table name; stopping")
      }

      # If the table does not exist yet, create it
      rs <- ROracle::dbSendQuery(
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

      # If the table does not exist yet, create it
      table_exists <- exists_table(
        connection_name = connection_name,
        table_name = table_name
      )

      conn <- get_db_conn(connection_name = connection_name)
      if (table_exists == TRUE) {
        execute_stmts(
          connection_name = connection_name,
          sql = tolower(stringr::str_glue("drop_{table_name}"))
        )

        ROracle::dbCommit(conn = conn)
        message(stringr::str_glue("+++ dropped table: {table_name}"))
      } else {
        message(stringr::str_glue("+++ table does not exist: {table_name}"))
        FALSE
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
    }
  )
}

#' Execute a Series of SQL Transactions
#'
#' @param connection_name The snake_case name of the connection.
#' @param sql_file the SQL part of the pattern: <SQL_DIR>/<CONNECTION_NAME>/<SQL>.sql
#' @param binds An optional tibble for specifying bind parameters; must have 0 or 1 rows
#' @param suppress_bind_logging Optionally suppress the logging of bind parameters (defaults to false).
#' @return Nothing
#' @export
execute_stmts <- function(binds = tibble::tibble(), connection_name, sql_file,
   suppress_bind_logging = FALSE) {
  connection_name <- toupper(connection_name)
  conn <- get_db_conn(connection_name = connection_name)
  binds <- binds %>%
    tibble::as_tibble() %>%
    dplyr::rename_with(toupper)
  stmts <- load_sql(
      sql_file,
      connection_name = connection_name
    ) %>%
    stringr::str_replace_all(pattern = "[ \t\r\n]+", replacement = " ") %>%
    stringr::str_split(pattern = ";;;")
  row_bind_ct <- dplyr::count(binds) %>% dplyr::pull()

  # Iterate over statements and excute them
  stmts[[1]] %>%
    lapply(function(stmt) {
      print(stringr::str_glue("... executing: {stmt}"))
      # If there are 0 or 1 rows of bind parameters, don't iterate over them;
      # otherwise, iterate over row binds within statements (statement-major)
      if (row_bind_ct < 2) {
        binds <- binds %>%
          dplyr::select(get_bind_colnames(stmt))
        # Only log binds if there are any
        if (row_bind_ct == 1) {
          if (suppress_bind_logging == FALSE) {
            message(paste0(c(
                "... binding: ",
                stringr::str_glue("{names(binds)} = {paste(binds)}; ")
            )))
          } else {
            message("... binding: <suppressed>")
          }
        }

        ROracle::dbSendQuery(
          conn = conn,
          statement = stmt,
          data = binds
        )
      } else {
        binds %>%
          as.list() %>%
          purrr::list_transpose() %>%
          lapply(function(row_binds) {
            row_binds <- row_binds %>%
              tibble::as_tibble() %>%
              dplyr::select(get_bind_colnames(stmt))
            if (suppress_bind_logging == FALSE) {
              message(paste0(c(
                  "... binding: ",
                  stringr::str_glue("{names(row_binds)} = {paste(row_binds)}; ")
              )))
            } else {
              message("... binding: <suppressed>")
            }
            ROracle::dbSendQuery(
              conn = conn,
              statement = stmt,
              data = row_binds
            )
          })
      }
  })

  # Finalize the work
  ROracle::dbCommit(conn = conn)
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
